from __future__ import annotations
#_statistics.py

from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import json, time, threading, re, os, tempfile

# ----- resolve CONFIG root safely -----
try:
    from cw_platform.config_base import CONFIG as _CONFIG_DIR  # type: ignore
    CONFIG = Path(_CONFIG_DIR)
except Exception:
    # Fallback to /config if available, else local folder
    try:
        CONFIG = Path(os.getenv("CW_CONFIG_DIR", "/config")).resolve()
    except Exception:
        CONFIG = Path(".").resolve()

STATS_PATH = CONFIG / "statistics.json"

# --- GUID patterns (robust to agent prefixes & query strings) ---
_GUID_TMDB_RE = re.compile(r"tmdb://(?:movie|tv)/(\d+)", re.IGNORECASE)
_GUID_IMDB_RE = re.compile(r"(tt\d{5,})", re.IGNORECASE)
_GUID_TVDB_RE = re.compile(r"tvdb://(\d+)", re.IGNORECASE)


def _read_json(p: Path) -> Dict[str, Any]:
    try:
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json_atomic(p: Path, data: Dict[str, Any]) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    tmp_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(p.parent),
            prefix=p.name + ".",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            json.dump(data, tmp, indent=2, ensure_ascii=False)
            tmp_name = tmp.name

        os.replace(tmp_name, p)

    except Exception:
        try:
            with p.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
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

    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = Path(path) if path else STATS_PATH
        self.lock = threading.Lock()
        self.data: Dict[str, Any] = {}
        self._load()

    # ---------- persistence ----------

    def _load(self) -> None:
        """Load or initialize the on-disk JSON state."""
        d = _read_json(self.path)
        d.setdefault("events", [])               # recent add/remove and other events
        d.setdefault("samples", [])              # rolling totals over time
        d.setdefault("current", {})              # current de-duped union map
        d.setdefault("counters", {"added": 0, "removed": 0})
        d.setdefault("last_run", {"added": 0, "removed": 0, "ts": 0})
        d.setdefault("http", {"events": [], "counters": {}, "last": {}})
        self.data = d

    def _save(self) -> None:
        """Persist in-memory state; never throw."""
        try:
            self.data["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            _write_json_atomic(self.path, self.data)
        except Exception:
            pass

    # ---------- identity helpers ----------

    @staticmethod
    def _title_of(d: Dict[str, Any]) -> str:
        return (d.get("title") or d.get("name") or d.get("original_title") or d.get("original_name") or "").strip()

    @staticmethod
    def _provider_items(state: Dict[str, Any], prov: str) -> Dict[str, Any]:
        """Read items from new snapshot layout with legacy fallback."""
        if not isinstance(state, dict):
            return {}
        P = (state.get("providers") or {}).get(prov.upper(), {}) or {}
        wl = (((P.get("watchlist") or {}).get("baseline") or {}).get("items") or {})
        if isinstance(wl, dict) and wl:
            return wl
        legacy = ((state.get(prov.lower(), {}) or {}).get("items") or {})
        return legacy if isinstance(legacy, dict) else {}

    @staticmethod
    def _year_of(d: Dict[str, Any]) -> Optional[int]:
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
    def _fallback_key(d: Dict[str, Any]) -> Optional[str]:
        t = Stats._title_of(d)
        if not t:
            return None
        y = Stats._year_of(d)
        return f"title:{t.lower()}:{y}" if y else f"title:{t.lower()}"

    @staticmethod
    def _extract_ids(d: Dict[str, Any]) -> Dict[str, Any]:
        """Extract common ids from nested/flat structures + robust Plex GUID parse."""
        out: Dict[str, Any] = {}
        ids = d.get("ids") or d.get("external_ids") or {}
        if isinstance(ids, dict):
            for k in ("imdb", "tmdb", "tvdb", "simkl", "slug"):
                v = ids.get(k)
                if v and k not in out:
                    out[k] = v

        # flat fallbacks
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

        # GUID parsing (handles agent prefixes and querystrings)
        guid = (d.get("guid") or d.get("Guid") or "").strip()
        if isinstance(guid, str) and "://" in guid:
            g = guid
            m = _GUID_IMDB_RE.search(g)
            if m and "imdb" not in out:
                out["imdb"] = m.group(1)
            m = _GUID_TMDB_RE.search(g)
            if m and "tmdb" not in out:
                out["tmdb"] = m.group(1)
            m = _GUID_TVDB_RE.search(g)
            if m and "tvdb" not in out:
                out["tvdb"] = m.group(1)
            # defensive numeric tail for tmdb://...
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
    def _canon_from_ids(ids: Dict[str, Any], typ: str) -> Optional[str]:
        """Canonical key: prefer TMDb, then IMDb, then TVDb, then slug; else None."""
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
    def _aliases(d: Dict[str, Any]) -> List[str]:
        """All identities this item could match on (tmdb/imdb/tvdb/slug + title/year)."""
        typ = (d.get("type") or "").lower()
        if typ in ("show", "tv"):
            typ = "tv"
        else:
            typ = "movie"
        ids = Stats._extract_ids(d)
        out: List[str] = []
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

    # ---------- union & counting ----------

    @staticmethod
    def _build_union_map(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Build a de-duplicated map of current items across providers."""
        plex     = Stats._provider_items(state, "PLEX")
        simkl    = Stats._provider_items(state, "SIMKL")
        trakt    = Stats._provider_items(state, "TRAKT")
        jellyfin = Stats._provider_items(state, "JELLYFIN")

        buckets: Dict[str, Dict[str, Any]] = {}
        alias2bucket: Dict[str, str] = {}

        def primary_key(d: Dict[str, Any]) -> str:
            typ = (d.get("type") or "").lower()
            typ = "tv" if typ in ("show", "tv") else "movie"
            ids = Stats._extract_ids(d)
            ck = Stats._canon_from_ids(ids, typ)
            return ck or (Stats._fallback_key(d) or f"fallback:{len(buckets)}")

        def ensure_bucket(d: Dict[str, Any]) -> str:
            for a in Stats._aliases(d):
                if a in alias2bucket:
                    return alias2bucket[a]
            pk = primary_key(d)
            if pk in buckets:
                pk = f"{pk}#{len(buckets)}"
            buckets[pk] = {
                "src": "",
                "title": Stats._title_of(d),
                "type": (d.get("type") or "").lower(),
                "p": False, "s": False, "t": False, "j": False,   # plex/simkl/trakt/jellyfin
            }
            for a in Stats._aliases(d):
                alias2bucket[a] = pk
            return pk

        def ingest(d: Dict[str, Any], flag: str):
            bk = ensure_bucket(d)
            if not buckets[bk].get("title"):
                buckets[bk]["title"] = Stats._title_of(d)
            if not buckets[bk].get("type"):
                buckets[bk]["type"] = (d.get("type") or "").lower()
            buckets[bk][flag] = True

        for _, raw in simkl.items():    ingest(raw, "s")
        for _, raw in plex.items():     ingest(raw, "p")
        for _, raw in trakt.items():    ingest(raw, "t")
        for _, raw in jellyfin.items(): ingest(raw, "j")

        # Legacy-compatible src label
        for b in buckets.values():
            p, s, t, j = bool(b.get("p")), bool(b.get("s")), bool(b.get("t")), bool(b.get("j"))
            if p and s: b["src"] = "both"       # historical meaning: Plex+Simkl
            elif p:     b["src"] = "plex"
            elif s:     b["src"] = "simkl"
            elif j:     b["src"] = "jellyfin"
            elif t:     b["src"] = "trakt"
            else:       b["src"] = ""

        return buckets

    def _counts_by_source(self, cur: Dict[str, Any]) -> Dict[str, int]:
        plex_only = simkl_only = trakt_only = jellyfin_only = both_ps = 0
        plex_total = simkl_total = trakt_total = jellyfin_total = 0

        for v in (cur or {}).values():
            p = bool((v or {}).get("p"))
            s = bool((v or {}).get("s"))
            t = bool((v or {}).get("t"))
            j = bool((v or {}).get("j"))

            plex_total     += 1 if p else 0
            simkl_total    += 1 if s else 0
            trakt_total    += 1 if t else 0
            jellyfin_total += 1 if j else 0

            # "both" remains legacy "Plex + Simkl"
            if p and s and not t and not j:
                both_ps += 1
            elif p and not s and not t and not j:
                plex_only += 1
            elif s and not p and not t and not j:
                simkl_only += 1
            elif t and not p and not s and not j:
                trakt_only += 1
            elif j and not p and not s and not t:
                jellyfin_only += 1

        # Preserve legacy keys; add jellyfin-specific counts
        return {
            "plex": plex_only,
            "simkl": simkl_only,
            "both": both_ps,
            "plex_total": plex_total,
            "simkl_total": simkl_total,
            "trakt_total": trakt_total,
            "jellyfin_total": jellyfin_total,

            "trakt": trakt_only,
            "jellyfin": jellyfin_only,
        }

    def _totals_from_events(self) -> Dict[str, int]:
        ev = list(self.data.get("events") or [])
        adds = sum(1 for e in ev if (e or {}).get("action") == "add")
        rems = sum(1 for e in ev if (e or {}).get("action") == "remove")
        return {"added": adds, "removed": rems}

    def _ensure_counters(self) -> Dict[str, int]:
        c = self.data.get("counters")
        if not isinstance(c, dict):
            c = self._totals_from_events()
            self.data["counters"] = {"added": int(c["added"]), "removed": int(c["removed"])}
        else:
            c.setdefault("added", 0)
            c.setdefault("removed", 0)
        return self.data["counters"]

    def _count_at(self, ts_floor: int) -> int:
        samples: List[Dict[str, Any]] = list(self.data.get("samples") or [])
        if not samples:
            return 0
        samples.sort(key=lambda r: int(r.get("ts") or 0))
        best = None
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

    # ---------- public API: state-based ----------

    def refresh_from_state(self, state: Dict[str, Any]) -> Dict[str, Any]:
        now_epoch = int(time.time())
        with self.lock:
            prev = {k: dict(v) for k, v in (self.data.get("current") or {}).items()}
            cur  = self._build_union_map(state)

            pk, ck = set(prev), set(cur)
            added_keys, removed_keys = sorted(ck - pk), sorted(pk - ck)

            ev = self.data.get("events") or []

            # --- de-noise re-keys (title match OR same id core) into a single "update"
            import re
            def _title_key(m): return (m.get("title") or "").strip().casefold()
            def _provset(m):  return {k for k in ("p","s","t","j") if m.get(k)} or {str(m.get("src") or "")}
            _IDCORE = re.compile(r"^(?P<p>[a-z0-9]+):(?:(?:movie|tv|show):)?(?P<i>[^:]+)$", re.I)
            def _idcore(k):
                m = _IDCORE.match(str(k) or ""); 
                return (m.group("p"), m.group("i")) if m else (None, None)

            for rk in list(removed_keys):
                rm = prev.get(rk) or {}
                rt, rp = _title_key(rm), _provset(rm)
                rp_name, rp_id = _idcore(rk)
                for ak in list(added_keys):
                    am = cur.get(ak) or {}
                    at, ap = _title_key(am), _provset(am)
                    ap_name, ap_id = _idcore(ak)

                    same_title = rt and at and (rt == at)
                    same_idcore = (rp_name == ap_name and rp_id and ap_id and rp_id == ap_id)
                    if (same_title or same_idcore) and (rp & ap or same_idcore):
                        # treat as rename/update (e.g., tmdb:movie→tmdb:tv, guid→tmdb)
                        removed_keys.remove(rk); added_keys.remove(ak)
                        ev.append({"ts": now_epoch, "action": "update", "key": ak,
                                "source": am.get("src",""), "title": am.get("title",""), "type": am.get("type","")})
                        break

            # --- true adds/removes after coalescing
            for k in added_keys:
                m = cur.get(k) or {}
                ev.append({"ts": now_epoch, "action": "add", "key": k,
                        "source": m.get("src",""), "title": m.get("title",""), "type": m.get("type","")})
            for k in removed_keys:
                m = prev.get(k) or {}
                ev.append({"ts": now_epoch, "action": "remove", "key": k,
                        "source": m.get("src",""), "title": m.get("title",""), "type": m.get("type","")})

            self.data["events"] = ev[-5000:]

            c = self._ensure_counters()
            c["added"]   = int(c.get("added", 0))   + len(added_keys)
            c["removed"] = int(c.get("removed", 0)) + len(removed_keys)
            self.data["counters"] = c

            self.data["last_run"] = {"added": len(added_keys), "removed": len(removed_keys), "ts": now_epoch}
            self.data["current"]  = cur

            samples = self.data.get("samples") or []
            samples.append({"ts": now_epoch, "count": len(cur)})
            self.data["samples"] = samples[-4000:]

            self._save()
            return {"now": len(cur), "week": self._count_at(now_epoch - 7*86400), "month": self._count_at(now_epoch - 30*86400)}

    def record_event(self, *, action: str, key: str, source: str = "", title: str = "", typ: str = "") -> None:
        now_epoch = int(time.time())
        with self.lock:
            ev = self.data.get("events") or []
            ev.append({"ts": now_epoch, "action": action, "key": key, "source": source, "title": title, "type": typ})
            self.data["events"] = ev[-5000:]
            self._save()

    def record_summary(self, added: int = 0, removed: int = 0) -> None:
        now_epoch = int(time.time())
        with self.lock:
            c = self._ensure_counters()
            c["added"] = int(c.get("added", 0)) + int(added or 0)
            c["removed"] = int(c.get("removed", 0)) + int(removed or 0)
            self.data["counters"] = c
            self.data["last_run"] = {"added": int(added or 0), "removed": int(removed or 0), "ts": now_epoch}
            self._save()

    def reset(self) -> None:
        with self.lock:
            self.data = {
                "events": [],
                "samples": [],
                "current": {},
                "counters": {"added": 0, "removed": 0},
                "last_run": {"added": 0, "removed": 0, "ts": 0},
                "http": {"events": [], "counters": {}, "last": {}},
            }
            self._save()

    def overview(self, state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        now_epoch = int(time.time())
        week_floor = now_epoch - 7 * 86400
        month_floor = now_epoch - 30 * 86400

        with self.lock:
            cur_map = dict(self.data.get("current") or {})
            if state:
                cur_map = self._build_union_map(state)

            counters = self._ensure_counters()
            last_run = self.data.get("last_run") or {}

            return {
                "ok": True,
                "generated_at": datetime.fromtimestamp(now_epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "now": len(cur_map),
                "week": self._count_at(week_floor),
                "month": self._count_at(month_floor),
                "added": int(counters.get("added", 0)),
                "removed": int(counters.get("removed", 0)),
                "new": int(last_run.get("added") or 0),
                "del": int(last_run.get("removed") or 0),
                "by_source": self._counts_by_source(cur_map),
                "window": {
                    "week_start": datetime.fromtimestamp(week_floor, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "month_start": datetime.fromtimestamp(month_floor, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            }

    # ---------- public API: HTTP telemetry ----------
    def record_http(
        self,
        *,
        provider: str,
        endpoint: Optional[str] = None,
        method: Optional[str] = None,
        status: int = 0,
        ok: bool = False,
        bytes_in: int = 0,
        bytes_out: int = 0,
        ms: int = 0,
        rate_remaining: Optional[int] = None,
        rate_reset_iso: Optional[str] = None,
        **kw,  # accept extra keys like fn= / path=
    ) -> None:
        """Record a single HTTP call; safe and lightweight."""
        # Map legacy argument names if provided
        if endpoint is None and "path" in kw:
            endpoint = kw.get("path")
        if method is None and ("fn" in kw or "verb" in kw):
            method = kw.get("fn") or kw.get("verb")

        now_epoch = int(time.time())
        evt = {
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

            # events ring buffer
            events: List[Dict[str, Any]] = list(http.get("events") or [])
            events.append(evt)
            http["events"] = events[-2000:]

            # provider-level counters
            prov = evt["provider"] or "UNKNOWN"
            ctr = http.get("counters") or {}
            pc = ctr.get(prov) or {
                "calls": 0, "ok": 0, "err": 0,
                "bytes_in": 0, "bytes_out": 0, "ms_sum": 0,
                "last_status": 0, "last_ok": False, "last_at": 0,
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

            # last snapshot by provider+endpoint
            last = http.get("last") or {}
            key = f"{prov} {evt['method']} {evt['endpoint']}"
            last[key] = evt
            http["last"] = last

            self._save()
