# _scheduling.py
from __future__ import annotations

import random
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional

try:
    from zoneinfo import ZoneInfo  # py39+
except Exception:
    ZoneInfo = None

DEFAULT_SCHEDULING: Dict[str, Any] = {
    "enabled": False,
    "mode": "disabled",          # disabled | hourly | every_n_hours | daily_time
    "every_n_hours": 2,
    "daily_time": "03:30",       # HH:MM (24h)
    "timezone": "Europe/Amsterdam",
    "jitter_seconds": 0,
}

# ---- helpers -------------------------------------------------------------

def _now_local_naive() -> datetime:
    return datetime.now()

def _tz_from_cfg(sch: Dict[str, Any]) -> Optional[Any]:
    name = (sch.get("timezone") or "").strip()
    if not name or ZoneInfo is None:
        return None
    try:
        return ZoneInfo(name)
    except Exception:
        return None

def _apply_jitter(dt_local: datetime, sch: Dict[str, Any]) -> datetime:
    try:
        js = int(sch.get("jitter_seconds") or 0)
    except Exception:
        js = 0
    if js <= 0:
        return dt_local
    return dt_local + timedelta(seconds=random.randint(0, js))

def _parse_hhmm(val: str) -> Optional[tuple[int, int]]:
    try:
        hh, mm = map(int, (val or "").strip().split(":"))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return hh, mm
    except Exception:
        pass
    return None

def merge_defaults(s: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(DEFAULT_SCHEDULING)
    if isinstance(s, dict):
        out.update({k: v for k, v in s.items() if v is not None})
    return out

def _align_next_hour_in_tz(now_tz: datetime) -> datetime:
    base = now_tz.replace(minute=0, second=0, microsecond=0)
    return base + timedelta(hours=1)

def _to_local_naive(dt_tzaware: datetime) -> datetime:
    return dt_tzaware.astimezone().replace(tzinfo=None)

def compute_next_run(now: datetime, sch: Dict[str, Any]) -> datetime:
    mode = (sch.get("mode") or "disabled").lower()
    if not sch.get("enabled") or mode == "disabled":
        return _now_local_naive() + timedelta(days=365 * 100)

    tz = _tz_from_cfg(sch)

    if mode == "hourly":
        if tz is not None:
            nxt_tz = _align_next_hour_in_tz(datetime.now(tz))
            return _apply_jitter(_to_local_naive(nxt_tz), sch)
        base = _now_local_naive().replace(minute=0, second=0, microsecond=0)
        return _apply_jitter(base + timedelta(hours=1), sch)

    if mode == "every_n_hours":
        try:
            n = max(1, int(sch.get("every_n_hours") or 2))
        except Exception:
            n = 2
        anchor = (now if isinstance(now, datetime) else _now_local_naive()).replace(second=0, microsecond=0)
        return _apply_jitter(anchor + timedelta(hours=n), sch)

    if mode == "daily_time":
        hh, mm = (_parse_hhmm((sch.get("daily_time") or "").strip()) or (3, 30))
        if tz is not None:
            base_tz = datetime.now(tz)
            today = base_tz.replace(hour=hh, minute=mm, second=0, microsecond=0)
            nxt_tz = today if today > base_tz else today + timedelta(days=1)
            return _apply_jitter(_to_local_naive(nxt_tz), sch)
        base = _now_local_naive()
        today = base.replace(hour=hh, minute=mm, second=0, microsecond=0)
        nxt = today if today > base else today + timedelta(days=1)
        return _apply_jitter(nxt, sch)

    return _now_local_naive() + timedelta(days=365 * 100)

# ---- scheduler -----------------------------------------------------------

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
        self._poke = threading.Event()
        self._lock = threading.Lock()
        self._status: Dict[str, Any] = {
            "running": False,
            "last_tick": 0,
            "last_run_ok": None,
            "last_run_at": 0,
            "next_run_at": 0,
            "next_run_iso": "",
            "last_error": "",
        }

    # config

    def _get_sched_cfg(self) -> Dict[str, Any]:
        cfg = self.load_config_cb() or {}
        return merge_defaults(cfg.get("scheduling") or {})

    def _set_sched_cfg(self, s: Dict[str, Any]) -> None:
        cfg = self.load_config_cb() or {}
        cfg["scheduling"] = merge_defaults(s or {})
        self.save_config_cb(cfg)

    def ensure_defaults(self) -> Dict[str, Any]:
        cfg = self.load_config_cb() or {}
        cfg["scheduling"] = merge_defaults(cfg.get("scheduling") or {})
        self.save_config_cb(cfg)
        return cfg["scheduling"]

    def set_enabled(self, enabled: bool) -> None:
        s = self._get_sched_cfg()
        s["enabled"] = bool(enabled)
        self._set_sched_cfg(s)
        self.refresh()

    def set_mode(self, *, mode: str, every_n_hours: Optional[int] = None, daily_time: Optional[str] = None) -> None:
        s = self._get_sched_cfg()
        s["mode"] = str(mode or "disabled").lower()
        if every_n_hours is not None:
            try:
                s["every_n_hours"] = max(1, int(every_n_hours))
            except Exception:
                pass
        if daily_time is not None:
            s["daily_time"] = str(daily_time).strip()
        self._set_sched_cfg(s)
        self.refresh()

    def status(self) -> Dict[str, Any]:
        with self._lock:
            st = dict(self._status)
        st["config"] = self._get_sched_cfg()
        return st

    # control

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._poke.clear()
        self._thread = threading.Thread(target=self._loop, name="SyncScheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._poke.set()
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=3.0)

    def refresh(self) -> None:
        self._poke.set()
        if not self._thread or not self._thread.is_alive():
            self.start()

    def trigger_once(self) -> None:
        if self.is_sync_running_fn():
            return
        ok, err = False, ""
        try:
            ok = bool(self.run_sync_fn())
        except Exception as e:
            ok, err = False, str(e)
        finally:
            with self._lock:
                self._status["last_run_ok"] = ok
                self._status["last_run_at"] = int(time.time())
                self._status["last_error"] = err

    # internals

    def _update_next(self, nxt: datetime) -> None:
        with self._lock:
            self._status["next_run_at"] = int(nxt.timestamp())
            try:
                iso = datetime.fromtimestamp(self._status["next_run_at"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                iso = ""
            self._status["next_run_iso"] = iso

    def _loop(self) -> None:
        with self._lock:
            self._status["running"] = True
        try:
            while not self._stop.is_set():
                now_local = _now_local_naive()
                sch = self._get_sched_cfg()
                nxt = compute_next_run(now_local, sch)
                self._update_next(nxt)

                while not self._stop.is_set():
                    with self._lock:
                        self._status["last_tick"] = int(time.time())

                    if not sch.get("enabled"):
                        self._sleep_or_poke(1.0)
                        break

                    if _now_local_naive() >= nxt:
                        if not self.is_sync_running_fn():
                            ok, err = False, ""
                            try:
                                ok = bool(self.run_sync_fn())
                            except Exception as e:
                                ok, err = False, str(e)
                            finally:
                                with self._lock:
                                    self._status["last_run_ok"] = ok
                                    self._status["last_run_at"] = int(time.time())
                                    self._status["last_error"] = err

                        now_local = _now_local_naive()
                        sch = self._get_sched_cfg()
                        nxt = compute_next_run(now_local, sch)
                        self._update_next(nxt)

                    remaining = max(0.0, (nxt - _now_local_naive()).total_seconds())
                    self._sleep_or_poke(min(30.0, remaining if remaining > 0 else 0.5))

                self._sleep_or_poke(0.2)
        finally:
            with self._lock:
                self._status["running"] = False

    def _sleep_or_poke(self, seconds: float) -> None:
        if seconds <= 0:
            return
        self._poke.wait(timeout=seconds)
        self._poke.clear()
