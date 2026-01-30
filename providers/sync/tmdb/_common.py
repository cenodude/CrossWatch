# /providers/sync/_common.py
# CrossWatch - Common TMDb utilities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from cw_platform.id_map import canonical_key, ids_from, minimal as id_minimal


def as_int(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(float(s)) if ("." in s or "e" in s.lower()) else int(s)
    except Exception:
        return None


def _norm_kind(t: Any) -> str:
    s = str(t or "").strip().lower()
    if s in ("tv", "show", "shows", "series"):
        return "tv"
    if s in ("movie", "movies"):
        return "movie"
    if s in ("episode", "episodes"):
        return "episode"
    if s in ("season", "seasons"):
        return "season"
    return s or "movie"


def key_of(item: Mapping[str, Any]) -> str:
    return str(canonical_key(id_minimal(item)) or "").strip()


def year_from_date(s: Any) -> int | None:
    txt = str(s or "").strip()
    if len(txt) >= 4 and txt[:4].isdigit():
        try:
            return int(txt[:4])
        except Exception:
            return None
    return None


def iso_z_from_tmdb(s: Any) -> str | None:
    txt = str(s or "").strip()
    if not txt:
        return None
    if txt.endswith("Z") and "T" in txt:
        return txt
    if txt.endswith(" UTC"):
        try:
            dt = datetime.strptime(txt, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return None
    try:
        dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def tmdb_id_from_item(item: Mapping[str, Any]) -> int | None:
    ids = ids_from(item)
    return as_int(ids.get("tmdb"))


def pick_media_type(item: Mapping[str, Any]) -> str:
    t = _norm_kind(item.get("type"))
    if t == "tv":
        return "tv"
    if t == "season":
        return "season"
    if t == "episode":
        return "episode"
    return "movie"


def pick_watchlist_media_type(item: Mapping[str, Any]) -> str:
    t = _norm_kind(item.get("type"))
    return "tv" if t in ("tv", "season", "episode") else "movie"


def unresolved_item(item: Mapping[str, Any], reason: str) -> dict[str, Any]:
    try:
        k = key_of(item)
    except Exception:
        k = ""
    return {"key": k, "reason": str(reason), "item": id_minimal(item)}


def resolve_tmdb_id(adapter: Any, item: Mapping[str, Any], *, want: str) -> int | None:
    want = _norm_kind(want)
    kind = _norm_kind(item.get("type"))

    if want == "tv" and kind in ("season", "episode"):
        show_ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else None
        if show_ids:
            tid = tmdb_id_from_item(show_ids)
            if tid is not None:
                return tid

    tid = tmdb_id_from_item(item)
    if tid is not None:
        return tid

    ids_src: Mapping[str, Any] = item
    if want == "tv" and kind in ("season", "episode"):
        show_ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else None
        if show_ids:
            ids_src = show_ids

    ids = ids_from(ids_src)
    imdb = ids.get("imdb")
    tvdb = ids.get("tvdb")
    if not imdb and not tvdb:
        return None

    memo = getattr(adapter, "_tmdb_find_memo", None)
    if not isinstance(memo, dict):
        memo = {}
        setattr(adapter, "_tmdb_find_memo", memo)

    if imdb:
        mkey = f"imdb:{imdb}|{want}"
        if mkey in memo:
            return memo[mkey]
        tid = _find_by_external(adapter, imdb, external_source="imdb_id", want=want)
        memo[mkey] = tid
        return tid

    if tvdb:
        mkey = f"tvdb:{tvdb}|{want}"
        if mkey in memo:
            return memo[mkey]
        tid = _find_by_external(adapter, tvdb, external_source="tvdb_id", want=want)
        memo[mkey] = tid
        return tid

    return None


def fetch_external_ids(adapter: Any, *, kind: str, tmdb_id: int, season: int | None = None, episode: int | None = None) -> dict[str, Any]:
    client = getattr(adapter, "client", None)
    if not client:
        return {}

    kind_n = _norm_kind(kind)
    if kind_n not in ("movie", "tv", "episode"):
        kind_n = "movie"

    memo = getattr(adapter, "_tmdb_external_ids_memo", None)
    if not isinstance(memo, dict):
        memo = {}
        setattr(adapter, "_tmdb_external_ids_memo", memo)

    skey = f"{kind_n}:{int(tmdb_id)}"
    if kind_n == "episode":
        s = int(season or 0)
        e = int(episode or 0)
        skey = f"{skey}:s{s}:e{e}"

    cached = memo.get(skey)
    if isinstance(cached, dict):
        return dict(cached)

    try:
        if kind_n == "movie":
            r = client.get(f"/movie/{int(tmdb_id)}/external_ids", params=client._params())
        elif kind_n == "tv":
            r = client.get(f"/tv/{int(tmdb_id)}/external_ids", params=client._params())
        else:

            if season is None or episode is None:
                memo[skey] = {}
                return {}
            r = client.get(
                f"/tv/{int(tmdb_id)}/season/{int(season)}/episode/{int(episode)}/external_ids",
                params=client._params(),
            )
    except Exception:
        memo[skey] = {}
        return {}

    if not (200 <= getattr(r, "status_code", 0) < 300):
        memo[skey] = {}
        return {}

    try:
        data = r.json() if (r.text or "").strip() else {}
    except Exception:
        data = {}
    if not isinstance(data, Mapping):
        data = {}

    imdb_id = data.get("imdb_id")
    tvdb_id = data.get("tvdb_id")

    out: dict[str, Any] = {"tmdb": int(tmdb_id)}
    if imdb_id:
        out["imdb"] = str(imdb_id)
    if tvdb_id is not None:
        out["tvdb"] = tvdb_id

    memo[skey] = dict(out)
    return out


def _find_by_external(adapter: Any, external_id: str, *, external_source: str, want: str) -> int | None:
    client = getattr(adapter, "client", None)
    if not client:
        return None

    url = f"{client.BASE}/find/{external_id}"
    try:
        r = client.get(url, params=client._params({"external_source": external_source}))
    except Exception:
        return None
    if not (200 <= r.status_code < 300):
        return None
    try:
        data = r.json() if (r.text or "").strip() else {}
    except Exception:
        data = {}
    if not isinstance(data, Mapping):
        return None

    want = _norm_kind(want)
    results = data.get("movie_results") if want == "movie" else data.get("tv_results")
    if not isinstance(results, list) or not results:
        return None
    first = results[0] if isinstance(results[0], Mapping) else None
    if not first:
        return None
    return as_int(first.get("id"))
