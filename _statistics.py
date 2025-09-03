# _statistics.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timezone
import json, time, threading

ROOT = Path(__file__).resolve().parent
CONFIG_BASE = Path("/config") if str(ROOT).startswith("/app") else ROOT
STATS_PATH = CONFIG_BASE / "statistics.json"

def _read_json(p: Path) -> Dict[str, Any]:
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _write_json_atomic(p: Path, data: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    tmp.replace(p)

class Stats:
    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = Path(path) if path else STATS_PATH
        self.lock = threading.Lock()
        self.data: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        d = _read_json(self.path)
        if not isinstance(d, dict):
            d = {}
        d.setdefault("events", [])
        d.setdefault("samples", [])
        d.setdefault("current", {})
        d.setdefault("counters", {"added": 0, "removed": 0})
        d.setdefault("last_run", {"added": 0, "removed": 0, "ts": 0})
        self.data = d

    def _save(self) -> None:
        self.data["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _write_json_atomic(self.path, self.data)

    @staticmethod
    def _title_of(d: Dict[str, Any]) -> str:
        return (d.get("title") or d.get("name") or d.get("original_title") or d.get("original_name") or "").strip()

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
    def _canon_from_ids(ids: Dict[str, Any], typ: str) -> Optional[str]:
        imdb = ids.get("imdb")
        if imdb and isinstance(imdb, str):
            imdb = imdb.lower()
            if not imdb.startswith("tt") and imdb.isdigit():
                imdb = f"tt{imdb}"
            return f"imdb:{imdb}"
        tmdb = ids.get("tmdb")
        if tmdb is not None:
            try:
                return f"tmdb:{(typ or 'movie').lower()}:{int(tmdb)}"
            except Exception:
                pass
        tvdb = ids.get("tvdb")
        if tvdb is not None:
            try:
                return f"tvdb:{int(tvdb)}"
            except Exception:
                pass
        simkl = ids.get("simkl")
        if simkl is not None:
            try:
                return f"simkl:{int(simkl)}"
            except Exception:
                pass
        return None

    @staticmethod
    def _extract_ids(d: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        ids = d.get("ids") or d.get("external_ids") or {}
        if isinstance(ids, dict):
            for k in ("imdb", "tmdb", "tvdb", "simkl"):
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
        guid = d.get("guid") or d.get("Guid") or ""
        if isinstance(guid, str) and "://" in guid:
            try:
                scheme, rest = guid.split("://", 1)
                scheme = scheme.lower()
                rest = rest.strip()
                if scheme == "imdb" and "imdb" not in out:
                    out["imdb"] = rest
                elif scheme == "tmdb" and "tmdb" not in out:
                    out["tmdb"] = rest
                elif scheme == "tvdb" and "tvdb" not in out:
                    out["tvdb"] = rest
            except Exception:
                pass
        return out

    @staticmethod
    def _union_keys(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        plex = (state.get("plex", {}) or {}).get("items", {}) or {}
        simkl = (state.get("simkl", {}) or {}).get("items", {}) or {}

        out: Dict[str, Dict[str, Any]] = {}
        simkl_index: Dict[str, Dict[str, Any]] = {}

        for _, raw in simkl.items():
            ids = Stats._extract_ids(raw)
            typ = (raw.get("type") or "").lower()
            ck = Stats._canon_from_ids(ids, typ) or Stats._fallback_key(raw)
            if not ck:
                continue
            simkl_index[ck] = {"src": "simkl", "title": Stats._title_of(raw), "type": typ}

        for _, raw in plex.items():
            ids = Stats._extract_ids(raw)
            typ = (raw.get("type") or "").lower()
            ck = Stats._canon_from_ids(ids, typ) or Stats._fallback_key(raw)
            if not ck:
                continue
            if ck in simkl_index:
                s = simkl_index.pop(ck)
                out[ck] = {
                    "src": "both",
                    "title": Stats._title_of(raw) or s.get("title") or "",
                    "type": typ or s.get("type") or "",
                }
            else:
                out[ck] = {"src": "plex", "title": Stats._title_of(raw), "type": typ}

        for ck, s in simkl_index.items():
            out[ck] = {"src": "simkl", "title": s.get("title") or "", "type": s.get("type") or ""}

        return out

    def _counts_by_source(self, cur: Dict[str, Any]) -> Dict[str, int]:
        plex_only = simkl_only = both = 0
        for v in (cur or {}).values():
            s = (v or {}).get("src") or ""
            if s == "plex": plex_only += 1
            elif s == "simkl": simkl_only += 1
            elif s == "both": both += 1
        return {
            "plex": plex_only,
            "simkl": simkl_only,
            "both": both,
            "plex_total": plex_only + both,
            "simkl_total": simkl_only + both,
        }

    def _totals_from_events(self) -> dict:
        ev = list(self.data.get("events") or [])
        adds = sum(1 for e in ev if (e or {}).get("action") == "add")
        rems = sum(1 for e in ev if (e or {}).get("action") == "remove")
        return {"added": adds, "removed": rems}

    def _ensure_counters(self) -> dict:
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

    def refresh_from_state(self, state: Dict[str, Any]) -> Dict[str, Any]:
        now = int(time.time())
        with self.lock:
            prev = {k: dict(v) for k, v in (self.data.get("current") or {}).items()}
            cur = self._union_keys(state)
            prev_keys, cur_keys = set(prev.keys()), set(cur.keys())
            added_keys = sorted(cur_keys - prev_keys)
            removed_keys = sorted(prev_keys - cur_keys)

            ev = self.data.get("events") or []
            for k in added_keys:
                m = cur.get(k) or {}
                ev.append({"ts": now, "action": "add", "key": k, "source": m.get("src",""), "title": m.get("title",""), "type": m.get("type","")})
            for k in removed_keys:
                m = prev.get(k) or {}
                ev.append({"ts": now, "action": "remove", "key": k, "source": m.get("src",""), "title": m.get("title",""), "type": m.get("type","")})
            self.data["events"] = ev[-5000:]

            c = self._ensure_counters()
            c["added"]   = int(c.get("added", 0))   + len(added_keys)
            c["removed"] = int(c.get("removed", 0)) + len(removed_keys)
            self.data["counters"] = c

            self.data["last_run"] = {"added": len(added_keys), "removed": len(removed_keys), "ts": now}
            self.data["current"] = cur

            samples = self.data.get("samples") or []
            samples.append({"ts": now, "count": len(cur)})
            self.data["samples"] = samples[-4000:]

            self._save()
            return {
                "now": len(cur),
                "week": self._count_at(now - 7*86400),
                "month": self._count_at(now - 30*86400),
            }

    def record_event(self, *, action: str, key: str, source: str = "", title: str = "", typ: str = "") -> None:
        now = int(time.time())
        with self.lock:
            ev = self.data.get("events") or []
            ev.append({"ts": now, "action": action, "key": key, "source": source, "title": title, "type": typ})
            self.data["events"] = ev[-5000:]
            # NOTE: no counters update here; counters are updated in refresh_from_state()
            self._save()


    def overview(self, state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        now_epoch = int(time.time())
        week_floor  = now_epoch - 7*86400
        month_floor = now_epoch - 30*86400

        with self.lock:
            cur_map = dict(self.data.get("current") or {})
            if state is not None:
                cur_map = self._union_keys(state)

            now_count   = len(cur_map)
            week_count  = self._count_at(week_floor)
            month_count = self._count_at(month_floor)

            counters = self._ensure_counters()
            last_run = self.data.get("last_run") or {}

            return {
                "ok": True,
                "generated_at": datetime.fromtimestamp(now_epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "now": now_count,
                "week": week_count,
                "month": month_count,
                "added": int(counters.get("added", 0)),
                "removed": int(counters.get("removed", 0)),
                "new": int(last_run.get("added") or 0),
                "del": int(last_run.get("removed") or 0),
                "by_source": self._counts_by_source(cur_map),
                "window": {
                    "week_start":  datetime.fromtimestamp(week_floor,  timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "month_start": datetime.fromtimestamp(month_floor, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            }
