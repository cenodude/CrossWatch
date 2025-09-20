"""
_scheduling.py

A tiny background scheduler driven by config callbacks, not file paths.
Safe-by-default: thread-safe, time zone aware for daily runs, and resilient
to bad config values. Designed to be embedded in long-running apps.

Public surface (unchanged where possible):
- merge_defaults(s: dict) -> dict
- compute_next_run(now: datetime, sch: dict) -> datetime
- class SyncScheduler:
    - start(), stop(), refresh()
    - ensure_defaults() -> dict
    - status() -> dict
    - (new) trigger_once(), set_enabled(bool), set_mode(...)

Notes:
- All wall-clock comparisons use naive local datetimes to avoid mixing aware
  and naive objects. Daily scheduling is computed in the configured time zone
  and converted back to local time for stable comparisons.
- `status()` exposes both epoch and ISO timestamps for UIs.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional
import random

try:
    # Python 3.9+ standard library
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover
    ZoneInfo = None  # graceful fallback if unavailable


DEFAULT_SCHEDULING: Dict[str, Any] = {
    "enabled": False,
    # "disabled" | "hourly" | "every_n_hours" | "daily_time"
    "mode": "disabled",
    "every_n_hours": 2,
    # HH:MM (24h) in `timezone`
    "daily_time": "03:30",
    # IANA time zone id; used for daily_time/hourly alignment
    "timezone": "Europe/Amsterdam",
    # optional jitter to spread start times (seconds)
    "jitter_seconds": 0,
}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _now_local_naive() -> datetime:
    """Return current time as naive local datetime."""
    # datetime.now() returns naive local by default
    return datetime.now()


def _tz_from_cfg(sch: Dict[str, Any]) -> Optional[Any]:
    """Resolve ZoneInfo from config, or None if not available/invalid."""
    tz_name = (sch.get("timezone") or "").strip()
    if not tz_name or ZoneInfo is None:
        return None
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return None


def _apply_jitter(dt_local: datetime, sch: Dict[str, Any]) -> datetime:
    """Optionally add a small random positive jitter (local naive)."""
    try:
        js = int(sch.get("jitter_seconds") or 0)
    except Exception:
        js = 0
    if js <= 0:
        return dt_local
    return dt_local + timedelta(seconds=random.randint(0, js))


def _parse_hhmm(val: str) -> Optional[tuple[int, int]]:
    """Parse 'HH:MM' (24h). Return (hh, mm) or None on error."""
    try:
        parts = (val or "").strip().split(":")
        hh, mm = int(parts[0]), int(parts[1])
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return hh, mm
    except Exception:
        pass
    return None


def merge_defaults(s: Dict[str, Any]) -> Dict[str, Any]:
    """Merge user scheduling config with defaults (shallow)."""
    out = dict(DEFAULT_SCHEDULING)
    if isinstance(s, dict):
        out.update({k: v for k, v in s.items() if v is not None})
    return out


def _align_next_hour_in_tz(now_tz: datetime) -> datetime:
    """Return top of next hour in the same TZ-aware time domain."""
    base = now_tz.replace(minute=0, second=0, microsecond=0)
    return base + timedelta(hours=1)


def _to_local_naive(dt_tzaware: datetime) -> datetime:
    """Convert TZ-aware datetime to naive local time."""
    return dt_tzaware.astimezone().replace(tzinfo=None)


def compute_next_run(now: datetime, sch: Dict[str, Any]) -> datetime:
    """
    Compute the next run time (naive local) based on scheduling config.
    - For 'daily_time' and 'hourly', alignment is done in the configured time
      zone, then converted to local time for stable comparisons.
    - For 'every_n_hours', we use a simple forward jump from 'now'.
    """
    mode = (sch.get("mode") or "disabled").lower()
    if not sch.get("enabled") or mode == "disabled":
        # Effectively never: far future date
        return _now_local_naive() + timedelta(days=365 * 100)

    tz = _tz_from_cfg(sch)

    if mode == "hourly":
        # Align to the next hour in configured TZ; fall back to local naive.
        if tz is not None:
            now_tz = datetime.now(tz)
            nxt_tz = _align_next_hour_in_tz(now_tz)
            return _apply_jitter(_to_local_naive(nxt_tz), sch)
        # Fallback: local naive alignment
        base = _now_local_naive().replace(minute=0, second=0, microsecond=0)
        return _apply_jitter(base + timedelta(hours=1), sch)

    if mode == "every_n_hours":
        # Simple delta from 'now' (use the 'now' passed by caller to preserve intent).
        n = 2
        try:
            n = int(sch.get("every_n_hours") or 2)
        except Exception:
            n = 2
        if n < 1:
            n = 1
        # Normalize seconds/micros down for cleanliness
        anchor = (now if isinstance(now, datetime) else _now_local_naive()).replace(second=0, microsecond=0)
        return _apply_jitter(anchor + timedelta(hours=n), sch)

    if mode == "daily_time":
        hhmm = (sch.get("daily_time") or "").strip()
        parsed = _parse_hhmm(hhmm)
        if tz is not None:
            # Compute in configured TZ, then convert to local naive
            base_tz = datetime.now(tz)
            if not parsed:
                hh, mm = 3, 30
            else:
                hh, mm = parsed
            today_target = base_tz.replace(hour=hh, minute=mm, second=0, microsecond=0)
            nxt_tz = today_target if today_target > base_tz else today_target + timedelta(days=1)
            return _apply_jitter(_to_local_naive(nxt_tz), sch)

        # Fallback: local naive
        if not parsed:
            hh, mm = 3, 30
        else:
            hh, mm = parsed
        base = _now_local_naive()
        today_target_local = base.replace(hour=hh, minute=mm, second=0, microsecond=0)
        nxt = today_target_local if today_target_local > base else today_target_local + timedelta(days=1)
        return _apply_jitter(nxt, sch)

    # Unknown mode → never
    return _now_local_naive() + timedelta(days=365 * 100)


# ---------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------

class SyncScheduler:
    """
    Small cooperative scheduler that periodically calls a provided `run_sync_fn`.
    It reloads scheduling preferences via `load_config` on every loop iteration.

    Concurrency:
    - Single background thread with a stop event.
    - `is_sync_running_fn` can be provided by the host to guard against overlap.
    - `refresh()` nudges the loop to re-read config and recompute the next slot.

    Time handling:
    - Internally compares naive local datetimes (consistent with datetime.now()).
    - Daily/hourly alignment respects the configured time zone if available.
    """

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
        self._poke = threading.Event()  # wakes the sleeper without stopping the loop
        self._lock = threading.Lock()
        self._status: Dict[str, Any] = {
            "running": False,
            "last_tick": 0,
            "last_run_ok": None,
            "last_run_at": 0,
            "next_run_at": 0,
            "next_run_iso": "",
        }

    # ---- config helpers ----

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

    # Convenience setters (optional to use)
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

    # ---- control ----

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._poke.clear()
        self._thread = threading.Thread(target=self._loop, name="SyncScheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._poke.set()  # make sure the loop wakes immediately
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=3.0)

    def refresh(self) -> None:
        """Ask the loop to wake, re-read config, and recompute next slot."""
        self._poke.set()
        if not self._thread or not self._thread.is_alive():
            self.start()

    def trigger_once(self) -> None:
        """Run the sync once ASAP, outside of the schedule."""
        if self.is_sync_running_fn():
            return
        ok = False
        try:
            ok = bool(self.run_sync_fn())
        finally:
            with self._lock:
                self._status["last_run_ok"] = ok
                self._status["last_run_at"] = int(time.time())

    # ---- internals ----

    def _update_next(self, nxt: datetime) -> None:
        with self._lock:
            self._status["next_run_at"] = int(nxt.timestamp())
            try:
                # present ISO in UTC for dashboards
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

                # Lightly spin until next slot or config change
                while not self._stop.is_set():
                    with self._lock:
                        self._status["last_tick"] = int(time.time())

                    # If disabled, sleep short and break to re-check config
                    if not sch.get("enabled"):
                        self._sleep_or_poke(1.0)
                        break

                    # Time to run?
                    if _now_local_naive() >= nxt:
                        if not self.is_sync_running_fn():
                            ok = False
                            try:
                                ok = bool(self.run_sync_fn())
                            finally:
                                with self._lock:
                                    self._status["last_run_ok"] = ok
                                    self._status["last_run_at"] = int(time.time())

                        # Recompute after a run in case mode/time changed during execution
                        now_local = _now_local_naive()
                        sch = self._get_sched_cfg()
                        nxt = compute_next_run(now_local, sch)
                        self._update_next(nxt)

                    # Sleep until close to the next slot, but wake early if poked
                    remaining = max(0.0, (nxt - _now_local_naive()).total_seconds())
                    self._sleep_or_poke(min(30.0, remaining if remaining > 0 else 0.5))

                # Outer loop small pause to avoid tight cycle on disable
                self._sleep_or_poke(0.2)
        finally:
            with self._lock:
                self._status["running"] = False

    def _sleep_or_poke(self, seconds: float) -> None:
        """Sleep for up to `seconds`, but return early if refreshed/stopped."""
        if seconds <= 0:
            return
        # Wait on poke or stop; whichever comes first
        self._poke.wait(timeout=seconds)
        # Clear poke so a subsequent refresh wakes us again
        self._poke.clear()
