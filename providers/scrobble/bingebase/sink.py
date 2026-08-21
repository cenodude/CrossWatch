# providers/scrobble/bingebase/sink.py
# CrossWatch - BingeBase realtime scrobble sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests

from cw_platform.config_base import load_config
from cw_platform.provider_instances import normalize_instance_id, resolve_provider_block
from services.activity import record_scrobble_event

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

try:
    from providers.scrobble.scrobble import ScrobbleSink, mask_account  # type: ignore
except ImportError:
    class ScrobbleSink:  # pragma: no cover
        def send(self, event: Any) -> None: ...

    def mask_account(value: Any) -> str:  # pragma: no cover
        s = str(value or "").strip()
        if not s:
            return "unknown"
        return s[0] + "*" if len(s) <= 2 else s[:2] + "***"


APP_AGENT = "CrossWatch/BingeBaseWebhook/1.0"
ACTION_EVENT = {
    "start": "playback.start",
    "pause": "playback.pause",
    "stop": "playback.stop",
}
ACTION_NOTIFICATION = {
    "start": "PlaybackStart",
    "pause": "PlaybackStart",
    "stop": "PlaybackStop",
}
KODI_WEBHOOK_PATH = "/webhooks/kodi/"


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


def _redact_url(url: Any) -> str:
    text = str(url or "").strip()
    if not text:
        return ""
    try:
        parts = urlsplit(text)
        if not parts.scheme or not parts.netloc:
            return "<redacted>"
        path = parts.path
        if parts.netloc.lower().endswith("bingebase.com") and path.startswith(KODI_WEBHOOK_PATH):
            path = KODI_WEBHOOK_PATH.rstrip("/")
        return urlunsplit((parts.scheme, parts.netloc, path, "", ""))
    except Exception:
        return "<redacted>"


def _is_kodi_webhook(url: Any) -> bool:
    try:
        parts = urlsplit(str(url or "").strip())
        return parts.netloc.lower().endswith("bingebase.com") and parts.path.startswith(KODI_WEBHOOK_PATH)
    except Exception:
        return False


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
    rest = text[2:]
    return text if rest.isdigit() and rest else None


def _provider_ids(ids: Mapping[str, Any], media_type: str) -> dict[str, str]:
    out: dict[str, str] = {}
    tmdb = _as_int(ids.get("tmdb"))
    imdb = _imdb(ids.get("imdb"))
    if tmdb and tmdb > 0:
        out["Tmdb"] = str(tmdb)
    if imdb:
        out["Imdb"] = imdb
    if media_type == "episode":
        show_tmdb = _as_int(ids.get("tmdb_show"))
        if show_tmdb and show_tmdb > 0:
            out["ShowTmdb"] = str(show_tmdb)
    return out


def _kodi_unique_ids(ids: Mapping[str, Any], media_type: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for name in ("tmdb", "tvdb", "imdb"):
        value = ids.get(f"{name}_episode") if media_type == "episode" else ids.get(name)
        value = value or ids.get(name)
        text = str(value or "").strip()
        if text:
            out[name] = text
    return out


def _kodi_show_unique_ids(ids: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for name in ("tmdb", "tvdb", "imdb"):
        text = str(ids.get(f"{name}_show") or "").strip()
        if text:
            out[name] = text
    return out


def _progress(value: Any) -> float:
    try:
        return max(0.0, min(100.0, float(value or 0.0)))
    except Exception:
        return 0.0


def _ms_to_seconds(value: Any) -> int | None:
    num = _as_int(value)
    if num is None or num <= 0:
        return None
    return max(1, int(round(num / 1000.0)))


def _route_source(cfg: Mapping[str, Any]) -> tuple[str, str]:
    watch = ((cfg.get("scrobble") or {}).get("watch") or {}) if isinstance(cfg, Mapping) else {}
    source = str(watch.get("route_provider") or "watcher").strip().lower() or "watcher"
    source_instance = str(watch.get("route_provider_instance") or "default").strip() or "default"
    return source, source_instance


class BingeBaseSink(ScrobbleSink):
    name = "bingebase"

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
            return resolve_provider_block(cfg, "bingebase", self.instance_id) or {}
        except Exception:
            block = cfg.get("bingebase") if isinstance(cfg, Mapping) else None
            return dict(block) if isinstance(block, Mapping) else {}

    def _jellyfin_payload(self, event: Any, action: str, media_type: str, ids: Mapping[str, Any], title: str) -> dict[str, Any] | None:
        provider_ids = _provider_ids(ids, media_type)
        if not provider_ids:
            _log("BINGEBASE: skip scrobble, no supported ids", "DEBUG")
            return None

        item: dict[str, Any] = {
            "Name": title,
            "Type": "Episode" if media_type == "episode" else "Movie",
            "ProviderIds": provider_ids,
        }
        year = _as_int(getattr(event, "year", None))
        if year and media_type == "movie":
            item["ProductionYear"] = year

        if media_type == "episode":
            season = _as_int(getattr(event, "season", None))
            episode = _as_int(getattr(event, "number", None))
            if season is None or episode is None:
                _log("BINGEBASE: skip scrobble, episode missing season/episode", "DEBUG")
                return None
            item["ParentIndexNumber"] = season
            item["IndexNumber"] = episode
            item["SeriesName"] = title

        return {
            "Event": ACTION_EVENT[action],
            "NotificationType": ACTION_NOTIFICATION[action],
            "Item": item,
            "Percentage": round(_progress(getattr(event, "progress", 0.0)), 1),
        }

    def _kodi_payload(self, event: Any, action: str, media_type: str, ids: Mapping[str, Any], title: str) -> dict[str, Any] | None:
        unique_ids = _kodi_unique_ids(ids, media_type)
        show_unique_ids = _kodi_show_unique_ids(ids) if media_type == "episode" else {}
        if not unique_ids and not show_unique_ids:
            _log("BINGEBASE: skip scrobble, no supported ids", "DEBUG")
            return None

        payload: dict[str, Any] = {
            "mediaType": media_type,
            "title": title,
            "uniqueIds": unique_ids,
            "event": action,
            "progress": {"percent": round(_progress(getattr(event, "progress", 0.0)), 1)},
        }
        year = _as_int(getattr(event, "year", None))
        if year:
            payload["year"] = year

        duration = _ms_to_seconds(getattr(event, "duration_ms", None))
        if duration:
            payload["duration"] = duration
            position = _ms_to_seconds(getattr(event, "position_ms", None))
            if position is None:
                position = int(round(duration * (_progress(getattr(event, "progress", 0.0)) / 100.0)))
            payload["progress"]["time"] = max(0, min(position, duration))

        if media_type == "episode":
            season = _as_int(getattr(event, "season", None))
            episode = _as_int(getattr(event, "number", None))
            if season is None or episode is None:
                _log("BINGEBASE: skip scrobble, episode missing season/episode", "DEBUG")
                return None
            payload["tvShowTitle"] = title
            payload["season"] = season
            payload["episode"] = episode
            payload["showUniqueIds"] = show_unique_ids

        return payload

    def _payload(self, event: Any, webhook_url: Any = None) -> dict[str, Any] | None:
        action = str(getattr(event, "action", "") or "").strip().lower()
        if action not in ACTION_EVENT:
            return None

        media_type = "episode" if str(getattr(event, "media_type", "") or "").lower() == "episode" else "movie"
        ids = getattr(event, "ids", None) or {}
        ids = ids if isinstance(ids, Mapping) else {}

        title = str(getattr(event, "title", "") or "").strip()
        if not title:
            _log("BINGEBASE: skip scrobble, missing title", "DEBUG")
            return None

        if _is_kodi_webhook(webhook_url):
            return self._kodi_payload(event, action, media_type, ids, title)
        return self._jellyfin_payload(event, action, media_type, ids, title)

    def send(self, event: Any, cfg: Mapping[str, Any] | None = None) -> None:
        cfgd = dict(cfg) if isinstance(cfg, Mapping) else self.config
        block = self._block(cfgd)
        webhook_url = str(block.get("webhook_url") or "").strip()
        if not webhook_url:
            _log("BINGEBASE: skip scrobble, webhook URL not configured", "DEBUG")
            return

        payload = self._payload(event, webhook_url)
        if payload is None:
            return

        try:
            timeout = float(block.get("timeout") or 20.0)
        except Exception:
            timeout = 20.0

        headers = {"Accept": "application/json", "Content-Type": "application/json", "User-Agent": APP_AGENT}
        api_key = str(block.get("api_key") or "").strip()
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        action = str(getattr(event, "action", "") or "").strip().lower()
        src, src_inst = _route_source(cfgd)
        try:
            resp = self.session.post(
                webhook_url,
                json=payload,
                headers=headers,
                timeout=max(1.0, timeout),
            )
        except Exception as exc:
            _log(f"BINGEBASE: scrobble {action} failed ({exc.__class__.__name__}) target={_redact_url(webhook_url)}", "WARN")
            return

        code = int(getattr(resp, "status_code", 0) or 0)
        if not (200 <= code < 300):
            _log(f"BINGEBASE: scrobble {action} rejected status={code} target={_redact_url(webhook_url)}", "WARN")
            return

        _log(
            f"BINGEBASE: scrobble {action} user='{mask_account(getattr(event, 'account', None))}' "
            f"p={_progress(getattr(event, 'progress', 0.0)):.1f}% source={src}:{src_inst}",
            "INFO",
        )
        if action == "stop":
            try:
                record_scrobble_event(
                    event,
                    source=src,
                    source_instance=src_inst,
                    target="bingebase",
                    target_instance=self.instance_id,
                    progress=_progress(getattr(event, "progress", 0.0)),
                )
            except Exception:
                pass


__all__ = ["BingeBaseSink", "_redact_url"]
