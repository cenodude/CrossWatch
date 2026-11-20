# providers/metadata/_meta_FANART.py
# CrossWatch - Fanart.tv artwork provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import OrderedDict
from concurrent.futures import Future
from pathlib import Path
from typing import Any, Callable, Mapping

import requests

from cw_platform.app_version import user_agent as http_user_agent

API_BASE = "https://webservice.fanart.tv/v3"
PROJECT_KEY = "4262eb9218c17879b61f5256f76c555f"
ASSET_HOST = "assets.fanart.tv"
TTL_FOUND = 7 * 86400
TTL_FOUND_PERSONAL = 86400
TTL_MISSING = 86400
TTL_TVDB = 30 * 86400
CACHE_MAX_ENTRIES = 1024

KINDS: dict[str, dict[str, tuple[str, ...]]] = {
    "movie": {
        "disc": ("moviedisc",),
        "logo": ("hdmovielogo", "movielogo"),
        "clearart": ("hdmovieclearart", "movieart"),
        "background": ("moviebackground", "movie4kbackground"),
        "poster": ("movieposter",),
        "banner": ("moviebanner",),
        "thumb": ("moviethumb",),
    },
    "tv": {
        "logo": ("hdtvlogo", "clearlogo"),
        "clearart": ("hdclearart", "clearart"),
        "background": ("showbackground", "show4kbackground"),
        "poster": ("tvposter",),
        "banner": ("tvbanner",),
        "thumb": ("tvthumb",),
        "character": ("characterart",),
        "season_poster": ("seasonposter",),
        "season_thumb": ("seasonthumb",),
        "season_banner": ("seasonbanner",),
    },
}
DISC_TYPES = ("bluray", "dvd", "3d")
_IMDB_RE = re.compile(r"^tt\d{5,12}$")
_NUM_RE = re.compile(r"^\d{1,10}$")
_CACHE_NAME_RE = re.compile(r"^[a-z]+_[a-z0-9]+(\.\d+\.\d+\.tmp|\.json)$")


def _entity(value: Any) -> str:
    v = str(value or "").strip().lower()
    return "movie" if v in {"movie", "movies", "film"} else "tv"


def _lookup_id(entity: str, ids: Mapping[str, Any]) -> str:
    if entity == "movie":
        tmdb = str(ids.get("tmdb") or "").strip()
        if _NUM_RE.fullmatch(tmdb):
            return tmdb
        imdb = str(ids.get("imdb") or "").strip().lower()
        return imdb if _IMDB_RE.fullmatch(imdb) else ""
    tvdb = str(ids.get("tvdb") or "").strip()
    return tvdb if _NUM_RE.fullmatch(tvdb) else ""


def _asset_url(value: Any) -> str:
    url = str(value or "").strip()
    if url.startswith("http://"):
        url = "https://" + url[7:]
    if not url.startswith(f"https://{ASSET_HOST}/"):
        return ""
    return url


def _likes(row: Mapping[str, Any]) -> int:
    try:
        return int(row.get("likes") or 0)
    except Exception:
        return 0


def _lang_rank(lang: str, want: str, textless: bool = False) -> int:
    if lang in {"", "00"}:
        return 0 if textless else 2
    if want and lang == want:
        return 0 if not textless else 1
    if lang == "en":
        return 1 if not textless else 2
    return 3


ART_PREFS = ("logos", "backgrounds")
ART_MODES = ("fanart", "tmdb_first", "tmdb")
_MODE_CODES = {"fanart": "1", "tmdb_first": "2"}


def _block(cfg: Mapping[str, Any] | None) -> Mapping[str, Any]:
    block = (cfg or {}).get("fanart")
    return block if isinstance(block, Mapping) else {}


def project_key(cfg: Mapping[str, Any] | None = None) -> str:
    own = str(_block(cfg).get("api_key") or "").strip()
    return own or str(os.environ.get("CW_FANART_PROJECT_KEY") or "").strip() or PROJECT_KEY


def enabled(cfg: Mapping[str, Any] | None) -> bool:
    return _block(cfg).get("enabled") is True and bool(project_key(cfg))


def art_mode(value: Any) -> str:
    if value is False:
        return "tmdb"
    if value is True or value is None:
        return "fanart"
    mode = str(value).strip().lower()
    return mode if mode in ART_MODES else "fanart"


def art_modes(cfg: Mapping[str, Any] | None) -> dict[str, str]:
    if not enabled(cfg):
        return {name: "tmdb" for name in ART_PREFS}
    block = _block(cfg)
    return {name: art_mode(block.get(name)) for name in ART_PREFS}


def art_prefs(cfg: Mapping[str, Any] | None) -> dict[str, bool]:
    return {name: mode != "tmdb" for name, mode in art_modes(cfg).items()}


def art_version(cfg: Mapping[str, Any] | None) -> str:
    parts = "".join(f"{name[0]}{_MODE_CODES[mode]}" for name, mode in art_modes(cfg).items() if mode in _MODE_CODES)
    return f"f{parts}" if parts else "t"


class FanartProvider:
    name = "FANART"
    art_only = True
    UA = http_user_agent(override_env="CW_FANART_UA")

    @staticmethod
    def manifest() -> dict[str, Any]:
        return {
            "id": "fanart",
            "name": "Fanart.tv",
            "enabled": True,
            "ready": None,
            "ok": None,
            "version": "1.0",
            "art_only": True,
        }

    def __init__(
        self,
        load_cfg: Callable[[], dict[str, Any]],
        save_cfg: Callable[[dict[str, Any]], None],
    ) -> None:
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self._cache: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()
        self._lock = threading.RLock()
        self._inflight: dict[str, Future[Any]] = {}
        self._disk_dir: Path | None = None

    def _keys(self) -> tuple[str, str]:
        cfg = self.load_cfg() or {}
        return project_key(cfg), str(_block(cfg).get("personal_api_key") or "").strip()

    def configured(self) -> bool:
        return enabled(self.load_cfg() or {})

    @staticmethod
    def _params(api_key: str, client_key: str) -> dict[str, str]:
        params: dict[str, str] = {}
        if api_key:
            params["api_key"] = api_key
        if client_key:
            params["client_key"] = client_key
        return params

    def clear_cache(self) -> None:
        with self._lock:
            self._cache.clear()

    def prefers(self, pref: str) -> bool:
        return bool(art_prefs(self.load_cfg() or {}).get(pref))

    def mode(self, pref: str) -> str:
        return art_modes(self.load_cfg() or {}).get(pref, "tmdb")

    def _coalesce(self, key: str, work: Callable[[], Any]) -> Any:
        with self._lock:
            future = self._inflight.get(key)
            owner = future is None
            if future is None:
                future = Future()
                self._inflight[key] = future
        if not owner:
            return future.result()
        try:
            result = work()
        except BaseException as exc:
            future.set_exception(exc)
            raise
        else:
            future.set_result(result)
            return result
        finally:
            with self._lock:
                if self._inflight.get(key) is future:
                    self._inflight.pop(key, None)

    def _cached(self, key: str, now: float, found_ttl: float | None = None) -> tuple[tuple[float, dict[str, Any]] | None, bool]:
        with self._lock:
            record = self._cache.get(key)
        if not self._fresh(record, now, found_ttl):
            disk = self._disk_read(key)
            if disk is not None:
                record = disk
                self._remember(key, disk)
        return record, self._fresh(record, now, found_ttl)

    def tvdb_for_tmdb(self, tmdb_id: Any, lookup: Callable[[], Any]) -> tuple[str, bool]:
        tmdb = str(tmdb_id or "").strip()
        if not _NUM_RE.fullmatch(tmdb):
            return "", True
        key = f"tmdbtv_{tmdb}"
        record, fresh = self._cached(key, time.time(), TTL_TVDB)
        if fresh:
            return str((record or (0, {}))[1].get("tvdb") or ""), True

        def work() -> tuple[str, bool]:
            current, current_fresh = self._cached(key, time.time(), TTL_TVDB)
            if current_fresh:
                return str((current or (0, {}))[1].get("tvdb") or ""), True
            try:
                tvdb = str(lookup() or "").strip()
            except Exception:
                return str((current or (0, {}))[1].get("tvdb") or ""), False
            now = time.time()
            data: dict[str, Any] = {"tvdb": tvdb} if _NUM_RE.fullmatch(tvdb) else {"_missing": True}
            self._remember(key, (now, data))
            self._disk_write(key, data, now)
            return str(data.get("tvdb") or ""), True

        return self._coalesce(key, work)

    def _disk_cache_dir(self) -> Path | None:
        if self._disk_dir is not None:
            return self._disk_dir
        try:
            import crosswatch as CW  # type: ignore

            base = Path(getattr(CW, "CACHE_DIR", None) or "")
        except Exception:
            base = Path("")
        if not str(base):
            try:
                from cw_platform.config_base import CONFIG

                base = Path(CONFIG) / "cache"
            except Exception:
                return None
        target = base / "meta" / "fanart"
        try:
            target.mkdir(parents=True, exist_ok=True)
        except Exception:
            return None
        self._disk_dir = target
        return target

    @staticmethod
    def _cache_file(target: Path, name: str) -> Path | None:
        if not _CACHE_NAME_RE.fullmatch(name):
            return None
        root = os.path.realpath(target)
        full = os.path.realpath(os.path.join(root, name))
        if not full.startswith(root + os.sep):
            return None
        return Path(full)

    def _disk_read(self, key: str) -> tuple[float, dict[str, Any]] | None:
        target = self._disk_cache_dir()
        if target is None:
            return None
        path = self._cache_file(target, f"{key}.json")
        if path is None:
            return None
        try:
            record = json.loads(path.read_text("utf-8"))
            data = record.get("data") if isinstance(record, dict) else None
            if not isinstance(data, dict):
                return None
            return float(record.get("ts") or 0), data
        except Exception:
            return None

    def _disk_write(self, key: str, data: dict[str, Any], fetched_at: float) -> None:
        target = self._disk_cache_dir()
        if target is None:
            return
        path = self._cache_file(target, f"{key}.json")
        tmp = self._cache_file(target, f"{key}.{os.getpid()}.{threading.get_ident()}.tmp")
        if path is None or tmp is None:
            return
        try:
            tmp.write_text(json.dumps({"ts": fetched_at, "data": data}, ensure_ascii=False), encoding="utf-8")
            tmp.replace(path)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    def _remember(self, key: str, record: tuple[float, dict[str, Any]]) -> None:
        with self._lock:
            self._cache[key] = record
            self._cache.move_to_end(key)
            while len(self._cache) > CACHE_MAX_ENTRIES:
                self._cache.popitem(last=False)

    def _fresh(self, record: tuple[float, dict[str, Any]] | None, now: float, found_ttl: float | None = None) -> bool:
        if record is None:
            return False
        ts, data = record
        if data.get("_missing"):
            ttl = TTL_MISSING
        elif found_ttl is not None:
            ttl = found_ttl
        else:
            ttl = TTL_FOUND_PERSONAL if self._keys()[1] else TTL_FOUND
        return now - ts < ttl

    def check_key(self, api_key: str, client_key: str = "") -> tuple[bool, str]:
        key = str(api_key or "").strip()
        personal = str(client_key or "").strip()
        checks = []
        if key:
            checks.append(("project API key", self._params(key, "")))
        if personal:
            checks.append(("personal API key", self._params(key or project_key({}), personal)))
        for label, params in checks:
            ok, error = self._check_params(params, label)
            if not ok:
                return ok, error
        return True, ""

    def _check_params(self, params: dict[str, str], label: str) -> tuple[bool, str]:
        try:
            r = requests.get(f"{API_BASE}/movies/603", params=params, headers={"User-Agent": self.UA}, timeout=12)
        except requests.exceptions.RequestException:
            return False, "Could not reach fanart.tv. Check network/DNS from the CrossWatch container."
        if r.status_code in (401, 403):
            return False, f"Invalid fanart.tv {label}"
        if r.status_code == 429:
            return False, "fanart.tv rate limit reached. Try again later."
        if r.status_code >= 500:
            return False, "fanart.tv server error. Try again later."
        if r.status_code != 200:
            return False, f"fanart.tv rejected the request (HTTP {r.status_code})"
        try:
            data = r.json()
        except Exception:
            data = None
        if not isinstance(data, dict) or not data or data.get("error") or str(data.get("status") or "").lower() == "error":
            return False, "fanart.tv returned an unexpected response"
        return True, ""

    def _fetch_raw(self, entity: str, lookup: str) -> dict[str, Any] | None:
        params = self._params(*self._keys())
        if not params:
            return None
        path = "movies" if entity == "movie" else "tv"
        r = requests.get(f"{API_BASE}/{path}/{lookup}", params=params, headers={"User-Agent": self.UA}, timeout=12)
        if r.status_code == 404:
            return {"_missing": True}
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, dict):
            return None
        if str(data.get("status") or "").lower() == "error" or not any(isinstance(v, list) and v for v in data.values()):
            return {"_missing": True}
        return data

    def lookup(self, entity: str, ids: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        ent = _entity(entity)
        lookup_id = _lookup_id(ent, ids or {})
        if not lookup_id or not self.configured():
            return {}, True
        key = f"{ent}_{lookup_id}"
        record, fresh = self._cached(key, time.time())
        if fresh:
            data = record[1] if record else {}
            return ({} if data.get("_missing") else data), True

        def work() -> tuple[dict[str, Any], bool]:
            current, current_fresh = self._cached(key, time.time())
            if not current_fresh:
                try:
                    fetched = self._fetch_raw(ent, lookup_id)
                except Exception:
                    fetched = None
                if fetched is None:
                    stale = current[1] if current else {}
                    return ({} if stale.get("_missing") else stale), False
                current = (time.time(), fetched)
                self._remember(key, current)
                self._disk_write(key, fetched, current[0])
            data = current[1] if current else {}
            return ({} if data.get("_missing") else data), True

        return self._coalesce(key, work)

    def raw(self, entity: str, ids: Mapping[str, Any]) -> dict[str, Any] | None:
        data, certain = self.lookup(entity, ids)
        return data if certain or data else None

    def images(self, entity: str, ids: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
        return self.images_from(entity, self.raw(entity, ids) or {})

    def images_from(self, entity: str, data: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
        ent = _entity(entity)
        if not data:
            return {}
        out: dict[str, list[dict[str, Any]]] = {}
        for kind, fields in KINDS[ent].items():
            rows: list[dict[str, Any]] = []
            for field in fields:
                arr = data.get(field)
                if not isinstance(arr, list):
                    continue
                for row in arr:
                    if not isinstance(row, Mapping):
                        continue
                    url = _asset_url(row.get("url"))
                    if not url:
                        continue
                    item: dict[str, Any] = {
                        "url": url,
                        "lang": str(row.get("lang") or "").strip().lower(),
                        "likes": _likes(row),
                        "field": field,
                    }
                    if row.get("disc_type"):
                        item["disc_type"] = str(row.get("disc_type")).strip().lower()
                    if row.get("disc") not in (None, ""):
                        item["disc"] = str(row.get("disc")).strip()
                    if row.get("season") not in (None, ""):
                        item["season"] = str(row.get("season")).strip()
                    rows.append(item)
            if rows:
                out[kind] = rows
        return out

    def pick(
        self,
        entity: str,
        ids: Mapping[str, Any],
        kind: str,
        *,
        lang: str = "",
        disc_type: str = "",
        season: int | None = None,
        textless: bool = False,
    ) -> dict[str, Any] | None:
        return self.pick_from(entity, self.raw(entity, ids) or {}, kind, lang=lang, disc_type=disc_type, season=season, textless=textless)

    def pick_from(
        self,
        entity: str,
        data: Mapping[str, Any],
        kind: str,
        *,
        lang: str = "",
        disc_type: str = "",
        season: int | None = None,
        textless: bool = False,
    ) -> dict[str, Any] | None:
        ent = _entity(entity)
        rows = self.images_from(ent, data).get(str(kind or "").strip().lower()) or []
        if season is not None:
            rows = [r for r in rows if r.get("season") in {str(season), "all"}]
        if not rows:
            return None
        want_lang = str(lang or "").strip().lower()[:2]
        want_disc = str(disc_type or "").strip().lower()
        field_order = {f: i for i, f in enumerate(KINDS[ent].get(str(kind).lower(), ()))}

        def rank(row: Mapping[str, Any]) -> tuple[int, int, int, int, int]:
            disc_miss = 0 if not want_disc or row.get("disc_type") == want_disc else 1
            disc_no = 0 if str(row.get("disc") or "1") == "1" else 1
            return (
                disc_miss,
                disc_no,
                field_order.get(str(row.get("field")), 9),
                _lang_rank(str(row.get("lang") or ""), want_lang, textless),
                -int(row.get("likes") or 0),
            )

        return sorted(rows, key=rank)[0]


def build(
    load_cfg: Callable[[], dict[str, Any]],
    save_cfg: Callable[[dict[str, Any]], None],
) -> FanartProvider:
    return FanartProvider(load_cfg, save_cfg)


PROVIDER = FanartProvider


def html() -> str:
    return '<div class="section" id="sec-fanart-raw" hidden></div>'
