# /cw_platform/anime_mapping/auto_update.py
# CrossWatch - Automatic Anime Mapping Updater
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

from _logging import log

from .updater import status as mapping_status, update as mapping_update

MIN_REFRESH_SECONDS = 3600
MAX_SLEEP_SECONDS = 600


class AnimeMappingAutoUpdater:
    def __init__(self, load_config: Callable[[], dict[str, Any]]) -> None:
        self.load_config = load_config
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._poke = threading.Event()
        self._lock = threading.Lock()
        self._status: dict[str, Any] = {
            "running": False,
            "enabled": False,
            "last_check_at": 0,
            "last_update_at": 0,
            "next_check_at": 0,
            "last_error": "",
        }

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                self._poke.set()
                return
            self._stop.clear()
            self._poke.clear()
            self._thread = threading.Thread(target=self._loop, name="AnimeMappingAutoUpdater", daemon=True)
            self._thread.start()
        log("auto_update_thread_started", level="debug", module="ANIME_MAPPING")

    def stop(self) -> None:
        thread = self._thread
        if not thread or not thread.is_alive():
            self._thread = None
            with self._lock:
                self._status["running"] = False
                self._status["next_check_at"] = 0
            return
        self._stop.set()
        self._poke.set()
        thread.join(timeout=3.0)
        if self._thread is thread:
            self._thread = None
        log("auto_update_thread_stopped", level="debug", module="ANIME_MAPPING")

    def refresh(self) -> None:
        self._poke.set()
        self.start()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._status)

    def _cfg(self) -> tuple[dict[str, Any], dict[str, Any]]:
        cfg = self.load_config() or {}
        block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else {}
        return cfg, dict(block or {}) if isinstance(block, Mapping) else {}

    def _enabled(self, block: Mapping[str, Any]) -> bool:
        return bool(block.get("enabled", False)) and bool(block.get("auto_update", True))

    def _interval_seconds(self, block: Mapping[str, Any]) -> int:
        try:
            hours = int(block.get("refresh_hours", 24) or 24)
        except Exception:
            hours = 24
        return max(MIN_REFRESH_SECONDS, hours * 3600)

    def _set_status(self, **patch: Any) -> None:
        with self._lock:
            self._status.update(patch)

    def _loop(self) -> None:
        self._set_status(running=True)
        try:
            while not self._stop.is_set():
                now = int(time.time())
                try:
                    cfg, block = self._cfg()
                    enabled = self._enabled(block)
                    tag = str(block.get("release_tag") or "v3").strip() or "v3"
                    interval = self._interval_seconds(block)

                    if not enabled:
                        self._set_status(enabled=False, next_check_at=0, last_error="")
                        self._sleep(60.0)
                        continue

                    st = mapping_status(cfg=cfg)
                    last_checked = int(st.get("last_checked_at") or 0)
                    installed = bool(st.get("installed") and st.get("index_ready"))
                    due_at = 0 if not last_checked or not installed else last_checked + interval

                    self._set_status(enabled=True, last_check_at=last_checked, next_check_at=due_at)

                    if due_at and now < due_at:
                        self._sleep(min(MAX_SLEEP_SECONDS, max(1, due_at - now)))
                        continue

                    log(
                        "auto_update_started",
                        level="debug",
                        module="ANIME_MAPPING",
                        extra={
                            "release_tag": tag,
                            "reason": "missing_index" if not installed else "interval_due",
                        },
                    )
                    res = mapping_update(release_tag=tag, force=False)
                    next_check = int(time.time()) + interval
                    self._set_status(
                        last_check_at=int(time.time()),
                        last_update_at=int(time.time()) if bool(res.get("updated")) else int(st.get("last_updated_at") or 0),
                        next_check_at=next_check,
                        last_error="",
                    )
                    log(
                        "auto_update_finished",
                        level="debug",
                        module="ANIME_MAPPING",
                        extra={
                            "release_tag": tag,
                            "updated": bool(res.get("updated")),
                            "next_check_at": next_check,
                        },
                    )
                except Exception as exc:
                    self._set_status(last_error=str(exc), next_check_at=int(time.time()) + MAX_SLEEP_SECONDS)
                    log(
                        "auto_update_failed",
                        level="error",
                        module="ANIME_MAPPING",
                        extra={"error_type": exc.__class__.__name__, "error": str(exc)},
                    )
                    self._sleep(MAX_SLEEP_SECONDS)
                    continue

                self._sleep(min(MAX_SLEEP_SECONDS, self._interval_seconds(block)))
        finally:
            self._set_status(running=False)

    def _sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        self._poke.wait(timeout=seconds)
        self._poke.clear()


_WORKER: AnimeMappingAutoUpdater | None = None
_WORKER_LOCK = threading.Lock()


def configure(load_config: Callable[[], dict[str, Any]]) -> AnimeMappingAutoUpdater:
    global _WORKER
    with _WORKER_LOCK:
        if _WORKER is None:
            _WORKER = AnimeMappingAutoUpdater(load_config)
        else:
            _WORKER.load_config = load_config
        return _WORKER


def refresh_from_config(load_config: Callable[[], dict[str, Any]]) -> None:
    worker = configure(load_config)
    cfg = load_config() or {}
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else {}
    enabled = bool((block or {}).get("enabled", False)) and bool((block or {}).get("auto_update", True)) if isinstance(block, Mapping) else False
    if enabled:
        worker.refresh()
    else:
        worker.stop()


def stop() -> None:
    with _WORKER_LOCK:
        worker = _WORKER
    if worker is not None:
        worker.stop()


def status() -> dict[str, Any]:
    with _WORKER_LOCK:
        worker = _WORKER
    return worker.status() if worker is not None else {"running": False, "enabled": False, "next_check_at": 0}
