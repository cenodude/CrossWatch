#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
_scheduling.py

A tiny background scheduler driven by config callbacks, not file paths.
This avoids coupling with JSON vs YAML details in the host app.
"""

from __future__ import annotations
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional

DEFAULT_SCHEDULING = {
    "enabled": False,
    "mode": "disabled",              # "disabled" | "hourly" | "every_n_hours" | "daily_time"
    "every_n_hours": 2,
    "daily_time": "03:30",           # HH:MM (24h)
    "timezone": "Europe/Amsterdam",
}

def merge_defaults(s: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(DEFAULT_SCHEDULING)
    if isinstance(s, dict):
        out.update({k: v for k, v in s.items() if v is not None})
    return out

def compute_next_run(now: datetime, sch: Dict[str, Any]) -> datetime:
    mode = (sch.get("mode") or "disabled").lower()
    if not sch.get("enabled") or mode == "disabled":
        return now + timedelta(days=365*100)  # effectively never
    if mode == "hourly":
        return (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    if mode == "every_n_hours":
        n = int(sch.get("every_n_hours") or 2)
        if n < 1: n = 1
        return (now + timedelta(hours=n)).replace(second=0, microsecond=0)
    if mode == "daily_time":
        hhmm = (sch.get("daily_time") or "03:30").strip()
        try:
            hh, mm = [int(x) for x in hhmm.split(":")]
            today_target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if today_target <= now:
                return today_target + timedelta(days=1)
            return today_target
        except Exception:
            fb = now.replace(hour=3, minute=30, second=0, microsecond=0)
            if fb <= now: fb += timedelta(days=1)
            return fb
    return now + timedelta(days=365*100)

class SyncScheduler:
    def __init__(
        self,
        load_config: Callable[[], Dict[str, Any]],
        save_config: Callable[[Dict[str, Any]], None],
        run_sync_fn: Callable[[], bool],
        is_sync_running_fn: Optional[Callable[[], bool]] = None,
    ) -> None:
        self.load_config_cb = load_config
        self.save_config_cb = save_config
        self.run_sync_fn = run_sync_fn
        self.is_sync_running_fn = is_sync_running_fn or (lambda: False)

        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._status: Dict[str, Any] = {
            "running": False,
            "last_tick": 0,
            "last_run_ok": None,
            "last_run_at": 0,
            "next_run_at": 0,
        }

    # ---- config helpers ----
    def _get_sched_cfg(self) -> Dict[str, Any]:
        cfg = self.load_config_cb() or {}
        sch = merge_defaults(cfg.get("scheduling") or {})
        return sch

    def _set_sched_cfg(self, s: Dict[str, Any]) -> None:
        cfg = self.load_config_cb() or {}
        cfg["scheduling"] = merge_defaults(s or {})
        self.save_config_cb(cfg)

    def ensure_defaults(self) -> Dict[str, Any]:
        cfg = self.load_config_cb() or {}
        cfg["scheduling"] = merge_defaults(cfg.get("scheduling") or {})
        self.save_config_cb(cfg)
        return cfg["scheduling"]

    def status(self) -> Dict[str, Any]:
        with self._lock:
            st = dict(self._status)
        st["config"] = self._get_sched_cfg()
        return st

    # ---- control ----
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="SyncScheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=2.0)

    def refresh(self) -> None:
        # nudge loop
        self._stop.set()
        time.sleep(0.02)
        self._stop.clear()
        if not self._thread or not self._thread.is_alive():
            self.start()

    # ---- internals ----
    def _loop(self) -> None:
        with self._lock:
            self._status["running"] = True
        try:
            while not self._stop.is_set():
                now = datetime.now()
                sch = self._get_sched_cfg()
                nxt = compute_next_run(now, sch)
                with self._lock:
                    self._status["last_tick"] = int(time.time())
                    self._status["next_run_at"] = int(nxt.timestamp())

                while True:
                    if self._stop.is_set(): break
                    if not sch.get("enabled"):
                        time.sleep(1.0)
                        break
                    if datetime.now() >= nxt:
                        if not self.is_sync_running_fn():
                            ok = False
                            try:
                                ok = bool(self.run_sync_fn())
                            finally:
                                with self._lock:
                                    self._status["last_run_ok"] = ok
                                    self._status["last_run_at"] = int(time.time())
                        # compute next slot
                        nxt = compute_next_run(datetime.now(), sch)
                        with self._lock:
                            self._status["next_run_at"] = int(nxt.timestamp())
                    # sleep small
                    rem = max(0.0, (nxt - datetime.now()).total_seconds())
                    time.sleep(min(30.0, rem if rem > 0 else 0.5))
                time.sleep(0.2)
        finally:
            with self._lock:
                self._status["running"] = False
