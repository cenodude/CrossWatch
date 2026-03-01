# services/statistics.py
# CrossWatch - Statistics and Metrics Management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import difflib
import json
import os
import re
import tempfile
import threading
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from cw_platform.config_base import CONFIG as _CONFIG_DIR  # type: ignore
    CONFIG = Path(_CONFIG_DIR)
except Exception:
    CONFIG = Path(os.getenv("CW_CONFIG_DIR", "/config")).resolve()

STATS_PATH = CONFIG / "statistics.json"
REPORT_DIR = Path("/config/sync_reports")

_GUID_TMDB_RE = re.compile(r"tmdb://(?:movie|tv)/(\d+)", re.I)
_GUID_IMDB_RE = re.compile(r"(tt\d{5,})", re.I)
_GUID_TVDB_RE = re.compile(r"tvdb://(\d+)", re.I)


def _canon_feature(name: Any) -> str:
    s = (str(name or "")).strip().lower()
    if s in ("watch", "watched", "history", "scrobble", "scrobbling"):
        return "history"
    if s.startswith("rating"):
        return "ratings"
    if s.startswith("playlist"):
        return "playlists"
    if s in ("wl", "watchlist"):
        return "watchlist"
    return s if s in ("watchlist", "ratings", "history", "playlists") else "watchlist"


def _read_json(p: Path) -> dict[str, Any]:
    try:
        with p.open("r", encoding="utf-8") as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _write_json_atomic(p: Path, data: dict[str, Any]) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    tmp_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=str(p.parent),
            prefix=p.name + ".",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            json.dump(data, tmp, ensure_ascii=False, indent=2)
            tmp_name = tmp.name
        os.replace(tmp_name, p)
    except Exception:
        try:
            with p.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    finally:
        if tmp_name:
            try:
                if Path(tmp_name).exists():
                    os.unlink(tmp_name)
            except Exception:
                pass


class Stats:
    def __init__(self, path: Path | None = None) -> None:
        self.path = Path(path) if path else STATS_PATH
        self.lock = threading.Lock()
        self.data: dict[str, Any] = {}
        self._load()

    # load/save
    def _load(self) -> None:
        d = _read_json(self.path)
        d.setdefault("events", [])
        d.setdefault("samples", [])
        d.setdefault("current", {})
        d.setdefault("current_by_feature", {})
        d.setdefault("counters", {"added": 0, "removed": 0})
        d.setdefault("last_run", {"added": 0, "removed": 0, "ts": 0})
        d.setdefault("http", {"events": [], "counters": {}, "last": {}})
        d.setdefault("feature_totals", [])
        d.setdefault("ingested_runs", [])
        self.data = d

    def _save(self) -> None:
        try:
            self.data["generated_at"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            _write_json_atomic(self.path, self.data)
        except Exception:
            pass

    # helpers
    @staticmethod
    def _title_of(d: dict[str, Any]) -> str:
        return (
            d.get("title")
            or d.get("name")
            or d.get("original_title")
            or d.get("original_name")
            or ""
        ).strip()

    @staticmethod
    def _year_of(d: dict[str, Any]) -> int | None:
        y = d.get("year") or d.get("release_year") or d.get("first_air_year")
        if isinstance(y, int):
            return y
        for k in ("release_date", "first_air_date", "aired", "premiered", "date"):
            v = d.get(k)
            if isinstance(v, str) and len(v) >= 4 and v[:4].isdigit():
                try:
                    return int(v[:4])
                except Exception:
                    pass
        return None

    @staticmethod
    def _fallback_key(d: dict[str, Any]) -> str | None:
        t = Stats._title_of(d)
        if not t:
            return None
        y = Stats._year_of(d)
        return f"title:{t.lower()}:{y}" if y else f"title:{t.lower()}"

    @staticmethod
    def _provider_feature_items(
        state: dict[str, Any],
        prov: str,
        feature: str,
    ) -> dict[str, Any]:
        if not isinstance(state, dict):
            return {}
        providers = (state.get("providers") or {}).get(prov.upper(), {}) or {}
        base = (((providers.get(feature) or {}).get("baseline") or {}).get("items") or {})
        return base if isinstance(base, dict) else {}

    @staticmethod
    def _providers_in_state(state: dict[str, Any]) -> list[str]:
        providers = (state or {}).get("providers") or {}
        return sorted([k.upper() for k in providers.keys()]) if isinstance(providers, dict) else []

    @staticmethod
    def _extract_ids(d: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        ids = d.get("ids") or d.get("external_ids") or {}
        if isinstance(ids, dict):
            for k in ("tmdb", "imdb", "tvdb", "simkl", "slug"):
                v = ids.get(k)
                if v and k not in out:
                    out[k] = v
        for k in ("imdb", "imdb_id", "tt"):
            v = d.get(k)
            if v and "imdb" not in out:
                out["imdb"] = v
        for k in ("tmdb", "tmdb_id", "id_tmdb", "tmdb_movie", "tmdb_show"):
            v = d.get(k)
            if v and "tmdb" not in out:
                out["tmdb"] = v
        for k in ("tvdb", "tvdb_id"):
            v = d.get(k)
            if v and "tvdb" not in out:
                out["tvdb"] = v
        for k in ("simkl", "simkl_id"):
            v = d.get(k)
            if v and "simkl" not in out:
                out["simkl"] = v
        if "slug" not in out and isinstance(d.get("slug"), (str, int)):
            out["slug"] = d.get("slug")
        g = (d.get("guid") or d.get("Guid") or "").strip()
        if isinstance(g, str) and "://" in g:
            m = _GUID_IMDB_RE.search(g)
            if m and "imdb" not in out:
                out["imdb"] = m.group(1)
            m = _GUID_TMDB_RE.search(g)
            if m and "tmdb" not in out:
                out["tmdb"] = m.group(1)
            m = _GUID_TVDB_RE.search(g)
            if m and "tvdb" not in out:
                out["tvdb"] = m.group(1)
            if "tmdb" not in out and "tmdb://" in g.lower():
                try:
                    tail = g.split("tmdb://", 1)[1]
                    num = tail.split("/", 1)[-1].split("?", 1)[0]
                    if num.isdigit():
                        out["tmdb"] = num
                except Exception:
                    pass
        return out

    @staticmethod
    def _canon_from_ids(ids: dict[str, Any], typ: str) -> str | None:
        tmdb = ids.get("tmdb")
        if tmdb is not None:
            try:
                return f"tmdb:{(typ or 'movie').lower()}:{int(tmdb)}"
            except Exception:
                pass
        imdb = ids.get("imdb")
        if isinstance(imdb, str):
            imdb = imdb.lower()
            if not imdb.startswith("tt") and imdb.isdigit():
                imdb = f"tt{imdb}"
            return f"imdb:{imdb}"
        tvdb = ids.get("tvdb")
        if tvdb is not None:
            try:
                return f"tvdb:{int(tvdb)}"
            except Exception:
                pass
        slug = ids.get("slug")
        if isinstance(slug, (str, int)):
            return f"slug:{slug}"
        return None

    @staticmethod
    def _aliases(d: dict[str, Any]) -> list[str]:
        typ = (d.get("type") or "").lower()
        typ = "tv" if typ in ("show", "tv") else "movie"
        ids = Stats._extract_ids(d)
        out: list[str] = []
        tmdb = ids.get("tmdb")
        if tmdb is not None:
            try:
                out.append(f"tmdb:{typ}:{int(tmdb)}")
            except Exception:
                pass
        imdb = ids.get("imdb")
        if isinstance(imdb, str):
            imdb = imdb.lower()
            if not imdb.startswith("tt") and imdb.isdigit():
                imdb = f"tt{imdb}"
            out.append(f"imdb:{imdb}")
        tvdb = ids.get("tvdb")
        if tvdb is not None:
            try:
                out.append(f"tvdb:{int(tvdb)}")
            except Exception:
                pass
        slug = ids.get("slug")
        if isinstance(slug, (str, int)):
            out.append(f"slug:{slug}")
        fb = Stats._fallback_key(d)
        if fb:
            out.append(fb)
        return out

    # Map building
    def _build_union_map(
        self,
        state: dict[str, Any],
        feature: str = "watchlist",
    ) -> dict[str, dict[str, Any]]:
        providers = self._providers_in_state(state)
        buckets: dict[str, dict[str, Any]] = {}
        alias2bucket: dict[str, str] = {}

        def primary_key(d: dict[str, Any]) -> str:
            typ = (d.get("type") or "").lower()
            typ = "tv" if typ in ("show", "tv") else "movie"
            ck = self._canon_from_ids(self._extract_ids(d), typ)
            return ck or (self._fallback_key(d) or f"fallback:{len(buckets)}")

        def ensure_bucket(d: dict[str, Any]) -> str:
            for a in self._aliases(d):
                if a in alias2bucket:
                    return alias2bucket[a]
            pk = primary_key(d)
            if pk in buckets:
                pk = f"{pk}#{len(buckets)}"
            buckets[pk] = {
                "src": "",
                "title": self._title_of(d),
                "type": (d.get("type") or "").lower(),
                "providers": set(),
            }
            for a in self._aliases(d):
                alias2bucket[a] = pk
            return pk

        def ingest(d: dict[str, Any], pid: str) -> None:
            bk = ensure_bucket(d)
            b = buckets[bk]
            if not b.get("title"):
                b["title"] = self._title_of(d)
            if not b.get("type"):
                b["type"] = (d.get("type") or "").lower()
            (b["providers"]).add(pid.lower())

        for pid in providers:
            items = self._provider_feature_items(state, pid, feature)
            for _, raw in (items or {}).items():
                ingest(raw, pid)

        for b in buckets.values():
            provs = set(b.get("providers") or [])
            b["src"] = "both" if len(provs) >= 2 else (next(iter(provs)).lower() if provs else "")
            b["providers"] = sorted(provs)

        return buckets

    def _counts_by_source(self, cur: dict[str, Any]) -> dict[str, int]:
        out: dict[str, int] = {}
        seen_provs: set[str] = set()
        for v in (cur or {}).values():
            provs = {str(p).lower() for p in (v or {}).get("providers", [])}
            if not provs:
                src = str((v or {}).get("src") or "").lower()
                if src and src != "both":
                    provs = {src}
            seen_provs |= provs
        for p in seen_provs:
            out[p] = 0
            out[f"{p}_total"] = 0
        out["both"] = 0

        for v in (cur or {}).values():
            provs = {str(p).lower() for p in (v or {}).get("providers", [])}
            if not provs:
                src = str((v or {}).get("src") or "").lower()
                if src and src != "both":
                    provs = {src}
            if not provs:
                continue
            if len(provs) == 1:
                p = next(iter(provs))
                out[p] = out.get(p, 0) + 1
            else:
                out["both"] += 1
            for p in provs:
                out[f"{p}_total"] = out.get(f"{p}_total", 0) + 1
        return out

    def _totals_from_events(self) -> dict[str, int]:
        ev = list(self.data.get("events") or [])
        adds = sum(1 for e in ev if (e or {}).get("action") == "add")
        rems = sum(1 for e in ev if (e or {}).get("action") == "remove")
        return {"added": adds, "removed": rems}

    def _ensure_counters(self) -> dict[str, int]:
        c = self.data.get("counters")
        if not isinstance(c, dict):
            c = self._totals_from_events()
            self.data["counters"] = {
                "added": int(c["added"]),
                "removed": int(c["removed"]),
            }
        else:
            c.setdefault("added", 0)
            c.setdefault("removed", 0)
        return self.data["counters"]

    def _count_at(self, ts_floor: int) -> int:
        samples: list[dict[str, Any]] = list(self.data.get("samples") or [])
        if not samples:
            return 0
        samples.sort(key=lambda r: int(r.get("ts") or 0))
        best: dict[str, Any] | None = None
        for r in samples:
            t = int(r.get("ts") or 0)
            if t <= ts_floor:
                best = r
            else:
                break
        if best is None:
            best = samples[0]
        try:
            return int(best.get("count") or 0)
        except Exception:
            return 0

    # Feature totals
    def record_feature_totals(
        self,
        feature: str,
        *,
        added: int = 0,
        removed: int = 0,
        updated: int = 0,
        src: str = "",
        run_id: str | None = None,
        expand_events: bool = True,
    ) -> None:
        f = _canon_feature(feature)
        now_epoch = int(time.time())
        row = {
            "ts": now_epoch,
            "feature": f,
            "added": int(added) or 0,
            "removed": int(removed) or 0,
            "updated": int(updated) or 0,
            "src": (src or "").upper(),
            "run_id": run_id or "",
            "kind": "agg",
        }
        with self.lock:
            arr = self.data.setdefault("feature_totals", [])
            arr.append(row)
            if len(arr) > 400:
                del arr[:-400]
            if expand_events:
                ev = self.data.get("events") or []

                def _emit(n: int, act: str) -> None:
                    if n <= 0:
                        return
                    ts = now_epoch
                    for i in range(int(n)):
                        ev.append(
                            {
                                "ts": ts,
                                "action": act,
                                "feature": f,
                                "key": f"agg:{f}:{act}:{ts}:{i}",
                                "source": src or "",
                                "title": "",
                                "type": "",
                            }
                        )

                _emit(row["added"], "add")
                _emit(row["removed"], "remove")
                _emit(row["updated"], "update")
                self.data["events"] = ev[-5000:]
            self._save()

    # Report ingestion
    def _ingest_latest_report_features_once(self) -> None:
        try:
            if not (REPORT_DIR.exists() and REPORT_DIR.is_dir()):
                return
            files = sorted(
                REPORT_DIR.glob("sync-*.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )[:3]
            if not files:
                return
            with self.lock:
                seen: list[str] = list(self.data.get("ingested_runs") or [])
            for f in files:
                try:
                    j = json.loads(f.read_text("utf-8") or "{}")
                except Exception:
                    continue
                run_id = str(j.get("finished_at") or j.get("started_at") or f.name)
                if run_id in seen:
                    continue
                feats = j.get("features") or {}
                any_rec = False
                for name, lane in (feats or {}).items():
                    a = int((lane or {}).get("added") or 0)
                    r = int((lane or {}).get("removed") or 0)
                    u = int((lane or {}).get("updated") or 0)
                    if a or r or u:
                        self.record_feature_totals(
                            name,
                            added=a,
                            removed=r,
                            updated=u,
                            src="REPORT",
                            run_id=run_id,
                            expand_events=True,
                        )
                        any_rec = True
                    ev = self.data.get("events") or []
                    for kind, act in (
                        ("spotlight_add", "add"),
                        ("spotlight_remove", "remove"),
                        ("spotlight_update", "update"),
                    ):
                        for it in (lane.get(kind) or [])[:12]:
                            title = it.get("title") or ""
                            key = it.get("key") or f"spot:{_canon_feature(name)}:{act}:{run_id}:{title[:32]}"
                            typ = it.get("type") or ""
                            src = it.get("source") or "REPORT"
                            ts = int(it.get("ts") or time.time())
                            ev.append(
                                {
                                    "ts": ts,
                                    "action": act,
                                    "feature": _canon_feature(name),
                                    "key": key,
                                    "source": src,
                                    "title": title,
                                    "type": typ,
                                }
                            )
                    if ev:
                        self.data["events"] = ev[-5000:]
                if any_rec:
                    with self.lock:
                        seen.append(run_id)
                        self.data["ingested_runs"] = seen[-50:]
                        self._save()
                break
        except Exception:
            pass

    # Refresh from state
    def refresh_from_state(self, state: dict[str, Any]) -> dict[str, Any]:
        self._ingest_latest_report_features_once()
        now_epoch = int(time.time())
        with self.lock:
            ev = self.data.get("events") or []

            prev_wl = {k: dict(v) for k, v in (self.data.get("current") or {}).items()}
            cur_wl = self._build_union_map(state, "watchlist")

            def _norm_title(s: str) -> str:
                s = unicodedata.normalize("NFKD", s or "")
                s = "".join(ch for ch in s if not unicodedata.combining(ch)).casefold()
                s = re.sub(r"\([^)]*\)|\[[^\]]*\]", " ", s)
                s = s.replace("&", " and ")
                s = re.sub(r"[^a-z0-9]+", " ", s)
                s = re.sub(r"\s+", " ", s).strip()
                toks = s.split()
                if toks and toks[0] in {"the", "a", "an"}:
                    s = " ".join(toks[1:])
                return s

            def _title_key(m: dict[str, Any]) -> str:
                return _norm_title((m.get("title") or "").strip())

            def _similar(a: str, b: str) -> bool:
                if not a or not b:
                    return False
                if a == b:
                    return True
                ta, tb = set(a.split()), set(b.split())
                if ta and (len(ta & tb) / len(ta | tb)) >= 0.85:
                    return True
                return difflib.SequenceMatcher(None, a, b).ratio() >= 0.92

            def _titles_match_loose(rm: dict[str, Any], am: dict[str, Any]) -> bool:
                ra, aa = _title_key(rm), _title_key(am)
                if not ra or not aa:
                    return False
                ry, ay = self._year_of(rm), self._year_of(am)
                if isinstance(ry, int) and isinstance(ay, int) and ry != ay:
                    return False
                return ra == aa or _similar(ra, aa)

            def _provset(m: dict[str, Any]) -> set[str]:
                provs = {str(p).lower() for p in (m.get("providers") or [])}
                if not provs:
                    s = str(m.get("src") or "").lower()
                    if s and s != "both":
                        provs = {s}
                return provs

            _IDCORE = re.compile(
                r"^(?P<p>[a-z0-9]+):(?:(?:movie|tv|show):)?(?P<i>[^:]+)$",
                re.I,
            )

            def _idcore(k: str) -> tuple[str | None, str | None]:
                m = _IDCORE.match(str(k) or "")
                return (m.group("p"), m.group("i")) if m else (None, None)

            pk, ck = set(prev_wl), set(cur_wl)
            added_keys, removed_keys = sorted(ck - pk), sorted(pk - ck)

            for rk in list(removed_keys):
                rm = prev_wl.get(rk) or {}
                rp = _provset(rm)
                rp_name, rp_id = _idcore(rk)
                for ak in list(added_keys):
                    am = cur_wl.get(ak) or {}
                    ap = _provset(am)
                    ap_name, ap_id = _idcore(ak)
                    same_title = _titles_match_loose(rm, am)
                    same_idcore = (
                        rp_name == ap_name and rp_id and ap_id and rp_id == ap_id
                    )
                    if (same_title or same_idcore) and (rp & ap or same_idcore):
                        removed_keys.remove(rk)
                        added_keys.remove(ak)
                        ev.append(
                            {
                                "ts": now_epoch,
                                "action": "update",
                                "feature": "watchlist",
                                "key": ak,
                                "source": am.get("src", ""),
                                "title": am.get("title", ""),
                                "type": am.get("type", ""),
                            }
                        )
                        break

            for k in added_keys:
                m = cur_wl.get(k) or {}
                ev.append(
                    {
                        "ts": now_epoch,
                        "action": "add",
                        "feature": "watchlist",
                        "key": k,
                        "source": m.get("src", ""),
                        "title": m.get("title", ""),
                        "type": m.get("type", ""),
                    }
                )
            for k in removed_keys:
                m = prev_wl.get(k) or {}
                ev.append(
                    {
                        "ts": now_epoch,
                        "action": "remove",
                        "feature": "watchlist",
                        "key": k,
                        "source": m.get("src", ""),
                        "title": m.get("title", ""),
                        "type": m.get("type", ""),
                    }
                )

            self.data["current"] = cur_wl
            counters = self._ensure_counters()
            counters["added"] = int(counters.get("added", 0)) + len(added_keys)
            counters["removed"] = int(counters.get("removed", 0)) + len(removed_keys)
            self.data["counters"] = counters
            self.data["last_run"] = {
                "added": len(added_keys),
                "removed": len(removed_keys),
                "ts": now_epoch,
            }
            samples = self.data.get("samples") or []
            samples.append({"ts": now_epoch, "count": len(cur_wl)})
            self.data["samples"] = samples[-4000:]

            # feature lanes (history/ratings/playlists)
            feats = ("history", "ratings", "playlists")
            cur_by = dict(self.data.get("current_by_feature") or {})
            for feat in feats:
                prev_map = dict(cur_by.get(feat) or {})
                cur_map = self._build_union_map(state, feat)
                if not prev_map and not cur_map:
                    cur_by[feat] = {}
                    continue
                ap, rp = set(cur_map), set(prev_map)
                adds, rems = sorted(ap - rp), sorted(rp - ap)

                for rk in list(rems):
                    rm = prev_map.get(rk) or {}
                    rp = _provset(rm)
                    rp_name, rp_id = _idcore(rk)
                    for ak in list(adds):
                        am = cur_map.get(ak) or {}
                        ap = _provset(am)
                        ap_name, ap_id = _idcore(ak)
                        same_title = _titles_match_loose(rm, am)
                        same_idcore = (
                            rp_name == ap_name and rp_id and ap_id and rp_id == ap_id
                        )
                        if (same_title or same_idcore) and (rp & ap or same_idcore):
                            rems.remove(rk)
                            adds.remove(ak)
                            ev.append(
                                {
                                    "ts": now_epoch,
                                    "action": "update",
                                    "feature": feat,
                                    "key": ak,
                                    "source": am.get("src", ""),
                                    "title": am.get("title", ""),
                                    "type": am.get("type", ""),
                                }
                            )
                            break

                for k in adds:
                    m = cur_map.get(k) or {}
                    ev.append(
                        {
                            "ts": now_epoch,
                            "action": "add",
                            "feature": feat,
                            "key": k,
                            "source": m.get("src", ""),
                            "title": m.get("title", ""),
                            "type": m.get("type", ""),
                        }
                    )
                for k in rems:
                    m = prev_map.get(k) or {}
                    ev.append(
                        {
                            "ts": now_epoch,
                            "action": "remove",
                            "feature": feat,
                            "key": k,
                            "source": m.get("src", ""),
                            "title": m.get("title", ""),
                            "type": m.get("type", ""),
                        }
                    )
                cur_by[feat] = cur_map

            self.data["current_by_feature"] = cur_by
            self.data["events"] = (ev or [])[-5000:]
            self._save()
            return {
                "now": len(cur_wl),
                "week": self._count_at(now_epoch - 7 * 86400),
                "month": self._count_at(now_epoch - 30 * 86400),
            }

    # Recording events and summaries
    def record_event(
        self,
        *,
        action: str,
        key: str,
        source: str = "",
        title: str = "",
        typ: str = "",
        feature: str | None = None,
    ) -> None:
        now_epoch = int(time.time())
        with self.lock:
            ev = self.data.get("events") or []
            ev.append(
                {
                    "ts": now_epoch,
                    "action": str(action or ""),
                    "feature": _canon_feature(feature) if feature else None,
                    "key": key,
                    "source": source,
                    "title": title,
                    "type": typ,
                }
            )
            self.data["events"] = ev[-5000:]
            self._save()

    def record_summary(self, added: int = 0, removed: int = 0) -> None:
        now_epoch = int(time.time())
        with self.lock:
            counters = self._ensure_counters()
            counters["added"] = int(counters.get("added", 0)) + int(added or 0)
            counters["removed"] = int(counters.get("removed", 0)) + int(removed or 0)
            self.data["counters"] = counters
            self.data["last_run"] = {
                "added": int(added or 0),
                "removed": int(removed or 0),
                "ts": now_epoch,
            }
            self._save()
        self._ingest_latest_report_features_once()

    def reset(self) -> None:
        with self.lock:
            self.data = {
                "events": [],
                "samples": [],
                "current": {},
                "current_by_feature": {},
                "counters": {"added": 0, "removed": 0},
                "last_run": {"added": 0, "removed": 0, "ts": 0},
                "http": {"events": [], "counters": {}, "last": {}},
                "feature_totals": [],
                "ingested_runs": [],
            }
            self._save()

    # Overview snapshot
    def overview(self, state: dict[str, Any] | None = None) -> dict[str, Any]:
        self._ingest_latest_report_features_once()
        now_epoch = int(time.time())
        week_floor = now_epoch - 7 * 86400
        month_floor = now_epoch - 30 * 86400
        with self.lock:
            cur_map = dict(self.data.get("current") or {})
            if state:
                cur_map = self._build_union_map(state, "watchlist")
            counters = self._ensure_counters()
            last_run = self.data.get("last_run") or {}
            return {
                "ok": True,
                "generated_at": datetime.fromtimestamp(
                    now_epoch, timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "now": len(cur_map),
                "week": self._count_at(week_floor),
                "month": self._count_at(month_floor),
                "added": int(counters.get("added", 0)),
                "removed": int(counters.get("removed", 0)),
                "new": int(last_run.get("added") or 0),
                "del": int(last_run.get("removed") or 0),
                "by_source": self._counts_by_source(cur_map),
                "window": {
                    "week_start": datetime.fromtimestamp(
                        week_floor, timezone.utc
                    ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "month_start": datetime.fromtimestamp(
                        month_floor, timezone.utc
                    ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            }

    # HTTP recording
    def record_http(
        self,
        *,
        provider: str,
        endpoint: str | None = None,
        method: str | None = None,
        status: int = 0,
        ok: bool = False,
        bytes_in: int = 0,
        bytes_out: int = 0,
        ms: int = 0,
        rate_remaining: int | None = None,
        rate_reset_iso: str | None = None,
        **kw: Any,
    ) -> None:
        if endpoint is None and "path" in kw:
            endpoint = kw.get("path")
        if method is None and ("fn" in kw or "verb" in kw):
            method = kw.get("fn") or kw.get("verb")
        now_epoch = int(time.time())
        evt: dict[str, Any] = {
            "ts": now_epoch,
            "provider": str(provider or "").upper(),
            "endpoint": str(endpoint or ""),
            "method": str(method or "").upper(),
            "status": int(status or 0),
            "ok": bool(ok),
            "ms": int(ms or 0),
            "bytes_in": int(bytes_in or 0),
            "bytes_out": int(bytes_out or 0),
        }
        if rate_remaining is not None:
            evt["rate_remaining"] = int(rate_remaining)
        if rate_reset_iso:
            evt["rate_reset"] = rate_reset_iso
        with self.lock:
            http = self.data.get("http")
            if not isinstance(http, dict):
                http = {"events": [], "counters": {}, "last": {}}
                self.data["http"] = http
            events: list[dict[str, Any]] = list(http.get("events") or [])
            events.append(evt)
            http["events"] = events[-2000:]
            prov = evt["provider"] or "UNKNOWN"
            ctr = http.get("counters") or {}
            pc = ctr.get(prov) or {
                "calls": 0,
                "ok": 0,
                "err": 0,
                "bytes_in": 0,
                "bytes_out": 0,
                "ms_sum": 0,
                "last_status": 0,
                "last_ok": False,
                "last_at": 0,
                "last_rate_remaining": None,
            }
            pc["calls"] += 1
            pc["ok"] += 1 if evt["ok"] else 0
            pc["err"] += 0 if evt["ok"] else 1
            pc["bytes_in"] += evt["bytes_in"]
            pc["bytes_out"] += evt["bytes_out"]
            pc["ms_sum"] += evt["ms"]
            pc["last_status"] = evt["status"]
            pc["last_ok"] = evt["ok"]
            pc["last_at"] = now_epoch
            if "rate_remaining" in evt:
                pc["last_rate_remaining"] = evt["rate_remaining"]
            ctr[prov] = pc
            http["counters"] = ctr
            last = http.get("last") or {}
            key = f"{prov} {evt['method']} {evt['endpoint']}"
            last[key] = evt
            http["last"] = last
            self._save()