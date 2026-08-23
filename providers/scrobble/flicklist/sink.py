# CrossWatch - FlickList scrobble sink
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import requests

from cw_platform.config_base import load_config
from cw_platform.event_archive import record_watch
from cw_platform.local_db.ttl_dedupe import once_per_ttl
from cw_platform.provider_instances import normalize_instance_id, resolve_provider_block
from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids as _rm_across
from providers.scrobble._watched_gate import resolve_stop_action
from providers.sync.flicklist._common import URL_SCROBBLE, error_of, flicklist_request
from services.activity import record_scrobble_event

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

try:
    from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink, mask_account  # type: ignore
except ImportError:
    class ScrobbleSink:  # pragma: no cover
        def send(self, event: Any) -> None: ...

    class ScrobbleEvent:  # pragma: no cover
        ...

    def mask_account(value: Any) -> str:  # pragma: no cover
        s = str(value or "").strip()
        return s[:2] + "***" if len(s) > 2 else "unknown"

APP_AGENT = "CrossWatch/FlickListWatcher/1.0"
DEFAULT_WATCHED_AT = 90.0
_AR_TTL = 60


def _cfg() -> dict[str, Any]:
    try:
        return load_config() or {}
    except Exception:
        return {}


def _is_debug() -> bool:
    try:
        return bool((_cfg().get("runtime") or {}).get("debug"))
    except Exception:
        return False


def _log(msg: str, lvl: str = "INFO") -> None:
    level = (str(lvl) or "INFO").upper()
    if level == "DEBUG" and not _is_debug():
        return
    if BASE_LOG is not None:
        try:
            BASE_LOG(msg, level=level, module="SCROBBLE")
            return
        except Exception:
            pass
    print(f"[SCROBBLE] {level}: {msg}")


def _route_source(cfg: Mapping[str, Any]) -> tuple[str, str]:
    watch = ((cfg.get("scrobble") or {}).get("watch") or {}) if isinstance(cfg, Mapping) else {}
    source = str(watch.get("route_provider") or "watcher").strip().lower() or "watcher"
    source_instance = str(watch.get("route_provider_instance") or "default").strip() or "default"
    return source, source_instance


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _imdb(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if not text.startswith("tt"):
        text = f"tt{text.lstrip('t')}"
    return text if text[2:].isdigit() else None


def _norm_media_type(value: str) -> str:
    text = (value or "").strip().lower()
    if text.endswith("s"):
        text = text[:-1]
    return "show" if text in {"serie", "series", "tv"} else text


def _ar_seen(key: str) -> bool:
    try:
        return not once_per_ttl(None, "auto_remove_seen", key, ttl_seconds=_AR_TTL)
    except Exception:
        return False


def _auto_remove_enabled(cfg: Mapping[str, Any], media_type: str) -> bool:
    scrobble = (cfg.get("scrobble") or {}) if isinstance(cfg, Mapping) else {}
    watch = scrobble.get("watch") or {}
    route_opts = watch.get("route_options") if isinstance(watch.get("route_options"), Mapping) else {}
    mode = str((route_opts or {}).get("auto_remove_watchlist") or "inherit").strip().lower()
    if mode == "off":
        return False
    if not scrobble.get("delete_plex") and mode != "on":
        return False
    types = scrobble.get("delete_plex_types") or []
    mt = _norm_media_type(media_type)
    if isinstance(types, str):
        return _norm_media_type(types) == mt
    try:
        return mt in {_norm_media_type(str(x)) for x in types if str(x).strip()}
    except Exception:
        return False


def _auto_remove_across(event: Any, cfg: Mapping[str, Any], scope: str = "") -> None:
    media_type = "episode" if str(getattr(event, "media_type", "") or "").lower() == "episode" else "movie"
    if not _auto_remove_enabled(cfg, media_type):
        return
    ids = {k: str(v) for k, v in (getattr(event, "ids", None) or {}).items() if v}
    if not ids:
        return
    key = f"{scope}|{media_type}|" + ",".join(f"{k}={v}" for k, v in sorted(ids.items()))
    if _ar_seen(key):
        return
    try:
        _rm_across(ids, media_type, scope=scope)
    except Exception:
        pass


class FlickListSink(ScrobbleSink):
    name = "flicklist"

    def __init__(self, cfg_provider: Callable[[], dict[str, Any]] | None = None, instance_id: Any = None) -> None:
        self._cfg_provider = cfg_provider
        self.instance_id = normalize_instance_id(instance_id)
        self.session = requests.Session()
        try:
            self.session.headers.setdefault("Accept", "application/json")
            self.session.headers.setdefault("User-Agent", APP_AGENT)
        except Exception:
            pass

    @property
    def config(self) -> dict[str, Any]:
        if self._cfg_provider is not None:
            try:
                cfg = self._cfg_provider()
                if isinstance(cfg, Mapping):
                    return dict(cfg)
            except Exception:
                pass
        return _cfg()

    def _block(self, cfg: Mapping[str, Any]) -> dict[str, Any]:
        try:
            return resolve_provider_block(cfg, "flicklist", self.instance_id) or {}
        except Exception:
            block = cfg.get("flicklist") if isinstance(cfg, Mapping) else None
            return dict(block) if isinstance(block, Mapping) else {}

    def _watched_at(self, cfg: Mapping[str, Any]) -> float:
        try:
            sc = cfg.get("scrobble") if isinstance(cfg, Mapping) else {}
            value = ((sc or {}).get("flicklist") or {}).get("watched_at")
            if value is None:
                value = ((sc or {}).get("trakt") or {}).get("watched_at", DEFAULT_WATCHED_AT)
            return max(0.0, min(100.0, float(value)))
        except Exception:
            return DEFAULT_WATCHED_AT

    def _ids_payload(self, event: Any, media_type: str) -> dict[str, Any]:
        ids = getattr(event, "ids", None) or {}
        if media_type == "episode":
            show_ids = {k[: -len("_show")]: v for k, v in ids.items() if k.endswith("_show") and v}
            if not show_ids:
                return {}
            ids = show_ids
        out: dict[str, Any] = {}
        fldb = str(ids.get("fldb") or "").strip()
        if fldb:
            out["fldb"] = fldb
            return out
        tmdb = _as_int(ids.get("tmdb"))
        if tmdb and tmdb > 0:
            out["tmdb"] = tmdb
        imdb = _imdb(ids.get("imdb"))
        if imdb:
            out["imdb"] = imdb
        tvdb = _as_int(ids.get("tvdb"))
        if tvdb and tvdb > 0:
            out["tvdb"] = tvdb
        return out

    def _payload(self, event: Any) -> tuple[str, dict[str, Any]] | None:
        media_type = "episode" if str(getattr(event, "media_type", "") or "").lower() == "episode" else "movie"
        ids = self._ids_payload(event, media_type)
        if not ids:
            return None
        payload: dict[str, Any] = {"ids": ids, "media_type": "show" if media_type == "episode" else "movie"}
        if media_type == "episode":
            season = _as_int(getattr(event, "season", None))
            episode = _as_int(getattr(event, "number", None))
            if season is None or episode is None:
                return None
            payload["season"] = season
            payload["episode"] = episode
        try:
            payload["progress"] = max(0.0, min(100.0, float(getattr(event, "progress", 0.0) or 0.0)))
        except Exception:
            payload["progress"] = 0.0
        return media_type, payload

    def send(self, event: Any, cfg: Mapping[str, Any] | None = None) -> None:
        cfg = dict(cfg) if isinstance(cfg, Mapping) else self.config
        block = self._block(cfg)
        if not str(block.get("api_key") or block.get("access_token") or block.get("token") or "").strip():
            _log("FLICKLIST: skip scrobble, not connected", "DEBUG")
            return
        action = str(getattr(event, "action", "") or "").strip().lower()
        if action not in {"start", "pause", "stop"}:
            return
        built = self._payload(event)
        if not built:
            _log("FLICKLIST: skip scrobble, no supported ids", "DEBUG")
            return
        media_type, payload = built
        progress_pct = float(payload.get("progress") or 0.0)
        watched_at = self._watched_at(cfg)
        watched = action == "stop" and resolve_stop_action(progress_pct, watched_at) == "stop"
        self._post(cfg, event, action, payload, progress_pct, watched=watched, media_type=media_type)

    def _post(self, cfg: Mapping[str, Any], event: Any, action: str, payload: Mapping[str, Any], progress_pct: float, *, watched: bool, media_type: str) -> None:
        adapter = _Adapter(dict(cfg), self.instance_id, self.session)
        src, src_inst = _route_source(cfg)
        archived = action in ("start", "stop")
        try:
            resp = flicklist_request(adapter, "POST", URL_SCROBBLE.format(action=action), json=dict(payload))
        except Exception as exc:
            reason = exc.__class__.__name__
            _log(f"FLICKLIST: scrobble {action} failed ({reason})", "WARN")
            if archived:
                self._archive(event, action, src, src_inst, progress_pct, status="fail", reason=reason)
            return

        code = int(getattr(resp, "status_code", 0) or 0)
        if not (200 <= code < 300):
            reason = error_of(resp) or f"http:{code}"
            _log(f"FLICKLIST: scrobble {action} rejected status={code} error={reason}", "WARN")
            if archived:
                self._archive(event, action, src, src_inst, progress_pct, status="fail", reason=reason)
            return

        if archived:
            self._archive(event, action, src, src_inst, progress_pct)

        if action == "stop" and watched:
            try:
                record_scrobble_event(
                    event,
                    source=src,
                    source_instance=src_inst,
                    target="flicklist",
                    target_instance=self.instance_id,
                    progress=progress_pct,
                )
            except Exception:
                pass
            _auto_remove_across(event, cfg, scope=f"flicklist:{self.instance_id}")

        _log(
            f"FLICKLIST: scrobble {action} user='{mask_account(getattr(event, 'account', None))}' p={progress_pct:.1f}% media={media_type}",
            "DEBUG" if action == "pause" else "INFO",
        )

    def _archive(self, event: Any, action: str, src: str, src_inst: str, progress_pct: float, *, status: str = "", reason: str = "") -> None:
        try:
            extra: dict[str, Any] = {}
            if status:
                extra["status"] = status
            if reason:
                extra["reason"] = reason
            record_watch(
                event,
                action=action,
                source_provider=src,
                source_instance=src_inst,
                destination_provider="flicklist",
                destination_instance=self.instance_id,
                progress=progress_pct,
                **extra,
            )
        except Exception:
            pass


class _Adapter:
    def __init__(self, cfg: dict[str, Any], instance_id: str, session: requests.Session) -> None:
        self.config = cfg
        self.instance_id = instance_id
        self.session = session


__all__ = ["FlickListSink"]
