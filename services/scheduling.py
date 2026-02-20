# services/scheduling.py
# CrossWatch - Scheduling module for automated sync tasks
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import random
import threading
import time
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

try:
    from zoneinfo import ZoneInfo  # py39+
except Exception:
    ZoneInfo = None  # type: ignore[assignment]

def _env_timezone_name() -> str:
    tz = os.environ.get("TZ", "").strip()
    return tz

# safety config defaults due to autostart and potential for misconfiguration
DEFAULT_SCHEDULING: dict[str, Any] = {
    "enabled": False,
    "mode": "disabled",
    "every_n_hours": 12,
    "daily_time": "03:30",
    "timezone": "",
    "jitter_seconds": 0,
    "advanced": {
        "enabled": False,
        "jobs": [],
    },
}

def _now_ts() -> int:
    return int(time.time())


def _now_local_naive() -> datetime:
    return datetime.now()

def _tz_from_cfg(sch: dict[str, Any]) -> Any | None:
    name = (sch.get("timezone") or "").strip()
    if not name or ZoneInfo is None:
        return None
    try:
        return ZoneInfo(name)
    except Exception:
        return None

def _as_now_in_tz(tz: Any | None) -> datetime:
    if tz is None:
        return _now_local_naive()
    try:
        return datetime.now(tz).replace(tzinfo=None)
    except Exception:
        return _now_local_naive()


def _apply_jitter(dt_local: datetime, sch: dict[str, Any]) -> datetime:
    try:
        js = int(sch.get("jitter_seconds") or 0)
    except Exception:
        js = 0
    if js <= 0:
        return dt_local
    return dt_local + timedelta(seconds=random.randint(0, js))

def _parse_hhmm(val: str) -> tuple[int, int] | None:
    try:
        hh, mm = map(int, (val or "").strip().split(":"))
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return hh, mm
    except Exception:
        pass
    return None

def merge_defaults(s: dict[str, Any]) -> dict[str, Any]:
    out = dict(DEFAULT_SCHEDULING)
    if isinstance(s, dict):
        for k, v in s.items():
            if v is None:
                continue
            if k == "advanced":
                continue
            out[k] = v

        adv = s.get("advanced")
        if isinstance(adv, dict):
            out_adv = dict(DEFAULT_SCHEDULING["advanced"])
            for k, v in adv.items():
                if v is None:
                    continue
                out_adv[k] = v
            if not isinstance(out_adv.get("jobs"), list):
                out_adv["jobs"] = []
            out["advanced"] = out_adv
    if not isinstance(out.get("advanced"), dict):
        out["advanced"] = dict(DEFAULT_SCHEDULING["advanced"])
    return out

def _align_next_hour_in_tz(now_tz: datetime) -> datetime:
    base = now_tz.replace(minute=0, second=0, microsecond=0)
    return base + timedelta(hours=1)

def _to_local_naive(dt_tzaware: datetime) -> datetime:
    return dt_tzaware.astimezone().replace(tzinfo=None)

def compute_next_run(now: datetime, sch: dict[str, Any]) -> datetime:
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

def _normalize_job(j: dict[str, Any]) -> dict[str, Any]:
    days = j.get("days")
    if not isinstance(days, list):
        days = []
    days2: list[int] = []
    for d in days:
        try:
            n = int(d)
            if 1 <= n <= 7 and n not in days2:
                days2.append(n)
        except Exception:
            continue
    days2.sort()
    return {
        "id": str(j.get("id") or "").strip() or f"job_{_now_ts()}",
        "pair_id": (str(j.get("pair_id") or "").strip() or None),
        "at": (str(j.get("at") or "").strip() or None),
        "days": days2,
        "after": (str(j.get("after") or "").strip() or None),
        "active": bool(j.get("active", True)),
    }


def _iter_adv_jobs(sch: dict[str, Any]) -> list[dict[str, Any]]:
    adv = sch.get("advanced") or {}
    if not isinstance(adv, dict) or not adv.get("enabled"):
        return []
    jobs = adv.get("jobs") or []
    if not isinstance(jobs, list):
        return []
    out: list[dict[str, Any]] = []
    for raw in jobs:
        if not isinstance(raw, dict):
            continue
        j = _normalize_job(raw)
        if not j["active"]:
            continue
        if not j["pair_id"]:
            continue
        if not j["at"] or _parse_hhmm(j["at"]) is None:
            continue
        out.append(j)
    return out


def _job_due_today(now_local: datetime, job: dict[str, Any]) -> datetime | None:
    hhmm = _parse_hhmm(job.get("at") or "")
    if not hhmm:
        return None
    hh, mm = hhmm
    days = job.get("days") or []
    # Empty days = every day
    allowed = set(days) if days else {1, 2, 3, 4, 5, 6, 7}
    dow = now_local.weekday() + 1  # Mon=1 .. Sun=7
    if dow not in allowed:
        return None
    return now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)


def _next_job_time(now_local: datetime, job: dict[str, Any]) -> datetime | None:
    hhmm = _parse_hhmm(job.get("at") or "")
    if not hhmm:
        return None
    hh, mm = hhmm
    days = job.get("days") or []
    allowed = set(days) if days else {1, 2, 3, 4, 5, 6, 7}

    base = now_local.replace(second=0, microsecond=0)
    for off in range(0, 8):  # next 7 days inclusive
        d = base + timedelta(days=off)
        dow = d.weekday() + 1
        if dow not in allowed:
            continue
        cand = d.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if cand > base:
            return cand
    return None


def _topo_order(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {j["id"]: j for j in jobs}
    pending = list(jobs)
    out: list[dict[str, Any]] = []
    emitted: set[str] = set()

    while pending:
        progress = False
        for j in list(pending):
            dep = j.get("after")
            if not dep or dep not in by_id or dep in emitted:
                out.append(j)
                emitted.add(j["id"])
                pending.remove(j)
                progress = True
        if progress:
            continue
        out.extend(pending)
        break
    return out


class SyncScheduler:
    def __init__(
        self,
        load_config: Callable[[], dict[str, Any]],
        save_config: Callable[[dict[str, Any]], None],
        run_sync_fn: Callable[..., bool],
        is_sync_running_fn: Callable[[], bool] | None = None,
        log_fn: Callable[..., Any] | None = None,
    ) -> None:
        self.load_config_cb = load_config
        self.save_config_cb = save_config
        self.run_sync_fn = run_sync_fn
        self.is_sync_running_fn = is_sync_running_fn or (lambda: False)
        self.log_fn = log_fn

        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._poke = threading.Event()
        self._lock = threading.Lock()

        self._status: dict[str, Any] = {
            "running": False,
            "last_tick": 0,
            "last_run_ok": None,
            "last_run_at": 0,
            "next_run_at": 0,
            "next_run_iso": "",
            "last_error": "",
            "last_job_id": "",
            "last_pair_id": "",
            "effective_mode": "",
        }

        self._adv_last_key: dict[str, str] = {}
        self._last_logged_next: int = 0

        self._std_next_ts: int = 0
        self._std_cfg_key: str = ""

    def _log(self, msg: str, *, level: str = "INFO") -> None:
        if not self.log_fn:
            return
        try:
            self.log_fn(msg, level=level)
        except TypeError:
            try:
                self.log_fn(msg)
            except Exception:
                pass
        except Exception:
            pass

    def _get_sched_cfg(self) -> dict[str, Any]:
        cfg = self.load_config_cb() or {}
        return merge_defaults(cfg.get("scheduling") or {})

    def _set_sched_cfg(self, s: dict[str, Any]) -> None:
        cfg = self.load_config_cb() or {}
        cfg["scheduling"] = merge_defaults(s or {})
        self.save_config_cb(cfg)

    def ensure_defaults(self) -> dict[str, Any]:
        cfg = self.load_config_cb() or {}
        cfg["scheduling"] = merge_defaults(cfg.get("scheduling") or {})
        self.save_config_cb(cfg)
        return cfg["scheduling"]

    def _effective(self, sch: dict[str, Any]) -> dict[str, Any]:
        adv = sch.get("advanced") if isinstance(sch.get("advanced"), dict) else {}
        adv_enabled = bool((adv or {}).get("enabled"))
        std_enabled = bool(sch.get("enabled"))
        if adv_enabled:
            return {"enabled": True, "mode": "advanced"}
        if std_enabled and (sch.get("mode") or "").lower() != "disabled":
            return {"enabled": True, "mode": (sch.get("mode") or "every_n_hours").lower()}
        return {"enabled": False, "mode": "disabled"}

    def set_enabled(self, enabled: bool) -> None:
        s = self._get_sched_cfg()
        s["enabled"] = bool(enabled)
        self._set_sched_cfg(s)
        self.refresh()

    def set_mode(
        self,
        *,
        mode: str,
        every_n_hours: int | None = None,
        daily_time: str | None = None,
    ) -> None:
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

    def status(self) -> dict[str, Any]:
        with self._lock:
            st = dict(self._status)
        cfg = self._get_sched_cfg()
        st["config"] = cfg
        st["effective"] = self._effective(cfg)
        return st

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._poke.clear()
        self._thread = threading.Thread(target=self._loop, name="SyncScheduler", daemon=True)
        self._thread.start()
        self._log("scheduler thread started", level="INFO")

    def stop(self) -> None:
        self._stop.set()
        self._poke.set()
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=3.0)
        self._log("scheduler thread stopped", level="INFO")

    def refresh(self) -> None:
        self._poke.set()
        if not self._thread or not self._thread.is_alive():
            self.start()

    def trigger_payload(self, payload: dict[str, Any] | None = None) -> bool:
        if self.is_sync_running_fn():
            self._log("trigger skipped: sync already running", level="INFO")
            return False
        ok, err = False, ""
        try:
            if payload:
                ok = bool(self._run_sync(payload))
            else:
                ok = bool(self._run_sync(None))
        except Exception as e:
            ok, err = False, str(e)
        finally:
            with self._lock:
                self._status["last_run_ok"] = ok
                self._status["last_run_at"] = _now_ts()
                self._status["last_error"] = err
        return ok

    def _run_sync(self, payload: dict[str, Any] | None) -> bool:
        if payload is None:
            return bool(self.run_sync_fn())
        try:
            return bool(self.run_sync_fn(payload))
        except TypeError:
            return bool(self.run_sync_fn())

    def _update_next(self, nxt: datetime | None, *, effective_mode: str) -> None:
        with self._lock:
            self._status["effective_mode"] = effective_mode
            if nxt is None:
                self._status["next_run_at"] = 0
                self._status["next_run_iso"] = ""
                return
            self._status["next_run_at"] = int(nxt.timestamp())
            try:
                iso = datetime.fromtimestamp(self._status["next_run_at"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                iso = ""
            self._status["next_run_iso"] = iso

        nxt_ts = int(nxt.timestamp()) if nxt else 0
        if nxt_ts and abs(nxt_ts - self._last_logged_next) >= 60:
            self._last_logged_next = nxt_ts
            self._log(f"next run scheduled at {self._status['next_run_iso'] or nxt.isoformat()}", level="INFO")

    def _adv_next(self, sch: dict[str, Any], now_local: datetime, tz: Any | None) -> datetime | None:
        jobs = _iter_adv_jobs(sch)
        if not jobs:
            return None

        now = _as_now_in_tz(tz)
        base = now.replace(second=0, microsecond=0)
        today = base.date().isoformat()
        best: datetime | None = None
        for j in jobs:
            cand = _next_job_time(base, j)
            if cand is None:
                continue
            dep = j.get("after")
            if dep:
                dep_key = self._adv_last_key.get(dep, "")
                if not dep_key.startswith(today):
                    cand2 = _next_job_time(base + timedelta(days=1), j)
                    cand = cand2 or cand
            if best is None or cand < best:
                best = cand
        return _apply_jitter(best, sch) if best else None

    def _adv_due_jobs(self, sch: dict[str, Any], tz: Any | None) -> list[tuple[datetime, dict[str, Any]]]:
        jobs = _iter_adv_jobs(sch)
        if not jobs:
            return []

        now = _as_now_in_tz(tz)
        base = now.replace(second=0, microsecond=0)
        today = base.date().isoformat()

        due: list[tuple[datetime, dict[str, Any]]] = []
        for j in jobs:
            dt = _job_due_today(base, j)
            if dt is None:
                continue
            if dt > base:
                continue
            key = f"{today}@{j.get('at')}"
            if self._adv_last_key.get(j["id"]) == key:
                continue
            due.append((dt, j))

        due.sort(key=lambda x: (x[0], str(x[1].get("id") or "")))
        return due

    def _adv_run_due(self, sch: dict[str, Any], tz: Any | None) -> bool:
        due = self._adv_due_jobs(sch, tz)
        if not due:
            return False

        now = _as_now_in_tz(tz)
        today = now.date().isoformat()
        jobs = [j for _, j in due]
        ordered = _topo_order(jobs)
        executed: set[str] = set()
        ok_all = True

        for j in ordered:
            if self._stop.is_set():
                break

            dep = j.get("after")
            if dep and dep in {x.get("id") for x in jobs} and dep not in executed:
                continue

            # Wait until sync is free
            waited = 0
            while self.is_sync_running_fn() and not self._stop.is_set():
                if waited == 0:
                    self._log("advanced: sync is busy; waiting to run due job(s)", level="INFO")
                waited += 1
                self._sleep_or_poke(2.0)
                if self._poke.is_set():
                    self._poke.clear()
                    # re-check due after config change
                    return True

            payload = {
                "source": "scheduler",
                "scheduler_mode": "advanced",
                "job_id": j["id"],
                "pair_id": j["pair_id"],
            }

            self._log(f"advanced: running job {j['id']} for pair {j['pair_id']}", level="INFO")
            ok = self._run_sync(payload)
            if not ok:
                ok_all = False
                self._log(f"advanced: job {j['id']} failed", level="ERROR")
            else:
                self._log(f"advanced: job {j['id']} ok", level="INFO")

            with self._lock:
                self._status["last_run_ok"] = ok
                self._status["last_run_at"] = _now_ts()
                self._status["last_error"] = "" if ok else "advanced job failed"
                self._status["last_job_id"] = j["id"]
                self._status["last_pair_id"] = j["pair_id"] or ""

            self._adv_last_key[j["id"]] = f"{today}@{j.get('at')}"
            executed.add(j["id"])
        return ok_all

    def _std_run_due(self) -> bool:
        payload = {"source": "scheduler", "scheduler_mode": "standard"}
        self._log("standard: triggering sync run", level="INFO")
        ok = self._run_sync(payload)
        with self._lock:
            self._status["last_run_ok"] = ok
            self._status["last_run_at"] = _now_ts()
            self._status["last_error"] = "" if ok else "standard run failed"
            self._status["last_job_id"] = ""
            self._status["last_pair_id"] = ""
        self._log("standard: run ok" if ok else "standard: run failed", level="INFO" if ok else "ERROR")
        return ok

    def _loop(self) -> None:
        with self._lock:
            self._status["running"] = True
        try:
            while not self._stop.is_set():
                with self._lock:
                    self._status["last_tick"] = _now_ts()

                sch = self._get_sched_cfg()
                eff = self._effective(sch)
                tz = _tz_from_cfg(sch)

                if not eff["enabled"]:
                    self._update_next(None, effective_mode="disabled")
                    with self._lock:
                        self._std_next_ts = 0
                        self._std_cfg_key = ""
                    self._sleep_or_poke(1.0)
                    continue

                now_local = _as_now_in_tz(tz)
                if eff["mode"] == "advanced":
                    with self._lock:
                        self._std_next_ts = 0
                        self._std_cfg_key = ""

                    ran = self._adv_run_due(sch, tz)
                    now_local = _as_now_in_tz(tz)
                    nxt = self._adv_next(sch, now_local, tz)
                    self._update_next(nxt, effective_mode="advanced")

                    if ran:
                        # after running, re-evaluate
                        self._sleep_or_poke(0.5)
                        continue

                    remaining = max(0.0, (nxt - _as_now_in_tz(tz)).total_seconds()) if nxt else 1.0
                    self._sleep_or_poke(min(30.0, remaining if remaining > 0 else 0.5))
                    continue

                # standard
                std_key = "|".join([
                    str(eff.get("mode") or ""),
                    str(sch.get("enabled") or ""),
                    str(sch.get("mode") or ""),
                    str(sch.get("every_n_hours") or ""),
                    str(sch.get("daily_time") or ""),
                    str(sch.get("timezone") or ""),
                    str(sch.get("jitter_seconds") or ""),
                ])

                now_ts = _now_ts()
                with self._lock:
                    cached_key = self._std_cfg_key
                    cached_next = int(self._std_next_ts or 0)

                if cached_next <= 0 or cached_key != std_key:
                    nxt = compute_next_run(now_local, sch)
                    cached_next = int(nxt.timestamp())
                    with self._lock:
                        self._std_cfg_key = std_key
                        self._std_next_ts = cached_next
                else:
                    nxt = datetime.fromtimestamp(cached_next)

                self._update_next(nxt, effective_mode=eff["mode"])

                if now_ts >= cached_next:
                    if self.is_sync_running_fn():
                        self._log("standard: sync is busy; delaying scheduled run", level="INFO")
                        self._sleep_or_poke(2.0)
                        continue

                    self._std_run_due()

                    now_local = _as_now_in_tz(tz)
                    nxt2 = compute_next_run(now_local, sch)
                    with self._lock:
                        self._std_next_ts = int(nxt2.timestamp())
                    self._update_next(nxt2, effective_mode=eff["mode"])

                    self._sleep_or_poke(0.5)
                    continue

                remaining = max(0.0, (nxt - _as_now_in_tz(tz)).total_seconds())
                self._sleep_or_poke(min(30.0, remaining if remaining > 0 else 0.5))
        finally:
            with self._lock:
                self._status["running"] = False

    def _sleep_or_poke(self, seconds: float) -> None:
        if seconds <= 0:
            return
        self._poke.wait(timeout=seconds)
        self._poke.clear()
