# /api/schedulingAPI.py
# CrossWatch - Scheduling API for task management
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import time
from typing import Any, Callable

from fastapi import APIRouter, Body

router = APIRouter(prefix="/api/scheduling", tags=["scheduling"])


def _env() -> tuple[
    Callable[..., dict[str, Any]],
    Callable[..., None],
    Any,
    dict[str, Any],
    Callable[..., int],
    Callable[..., Any],
]:
    try:
        from cw_platform.config_base import (
            load_config as _load_cfg,
            save_config as _save_cfg,
        )
        from crosswatch import (
            scheduler as _scheduler,
            _SCHED_HINT,
            _compute_next_run_from_cfg,
            _UIHostLogger,
        )

        return (
            _load_cfg,
            _save_cfg,
            _scheduler,
            _SCHED_HINT,
            _compute_next_run_from_cfg,
            _UIHostLogger,
        )
    except Exception:
        def _load_cfg() -> dict[str, Any]:
            return {}

        def _save_cfg(*args: Any, **kwargs: Any) -> None:
            return None

        class _DummyScheduler:
            def status(self) -> dict[str, Any]:
                return {}

            def start(self) -> None:
                return None

            def stop(self) -> None:
                return None

            def refresh(self) -> None:
                return None

        def _compute_next_run_from_cfg(*args: Any, **kwargs: Any) -> int:
            return 0

        def _ui_host_logger(*_args: Any, **_kwargs: Any) -> Callable[..., None]:
            def _inner(*_a: Any, **_k: Any) -> None:
                return None
            return _inner

        return _load_cfg, _save_cfg, _DummyScheduler(), {}, _compute_next_run_from_cfg, _ui_host_logger


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _dedupe(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _clean_text(item)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _pair_maps(cfg: dict[str, Any]) -> tuple[set[str], set[str]]:
    known: set[str] = set()
    enabled: set[str] = set()
    for pair in _as_list(cfg.get("pairs")):
        if not isinstance(pair, dict):
            continue
        pair_id = _clean_text(pair.get("id") or pair.get("pair_id") or pair.get("name") or pair.get("label"))
        if not pair_id:
            continue
        known.add(pair_id)
        if pair.get("enabled", True) is not False:
            enabled.add(pair_id)
    return known, enabled


def _blank_adv_job(job: dict[str, Any]) -> bool:
    days = _as_list(job.get("days"))
    return (
        not _clean_text(job.get("pair_id"))
        and not _clean_text(job.get("at"))
        and not _clean_text(job.get("after"))
        and not days
    )


def _blank_workflow(workflow: dict[str, Any]) -> bool:
    return not any(_clean_text(step.get("pair_id")) for step in _as_list(workflow.get("steps")) if isinstance(step, dict))


def _scrobble_event_route_ids(cfg: dict[str, Any]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {"watcher": set(), "webhook": set()}
    try:
        from providers.scrobble.routes import normalize_routes
        from providers.scrobble.sources import scrobble_sources

        sc = _as_dict(cfg.get("scrobble"))
        sources = scrobble_sources(sc)
        if sources.get("watcher"):
            for route in normalize_routes(cfg):
                if not isinstance(route, dict) or route.get("enabled") is False:
                    continue
                route_id = _clean_text(route.get("id"))
                provider = _clean_text(route.get("provider"))
                sink = _clean_text(route.get("sink"))
                if route_id and provider and sink:
                    out["watcher"].add(route_id)
        if sources.get("webhook"):
            out["webhook"].update({"plex", "jellyfin", "emby"})
    except Exception:
        pass
    return out


def _scheduling_warning_summary(cfg: dict[str, Any]) -> dict[str, Any]:
    scfg = _as_dict(cfg.get("scheduling"))
    adv = _as_dict(scfg.get("advanced"))
    warnings: list[str] = []

    if bool(scfg.get("enabled")) and not bool(adv.get("enabled")):
        mode = _clean_text(scfg.get("mode")).lower()
        if mode in {"custom", "custom_interval"}:
            try:
                minutes = max(15, int(scfg.get("custom_interval_minutes") or 60))
            except Exception:
                minutes = 60
            if minutes < 60:
                warnings.append(
                    "Standard schedule: Custom schedules shorter than 1 hour can be seen as abusing trackers API's and may result in a ban. Use them carefully."
                )

    if not bool(adv.get("enabled")):
        return {"warning": bool(warnings), "warnings": _dedupe(warnings)}

    _, enabled_pairs = _pair_maps(cfg)
    jobs = _as_list(adv.get("jobs"))
    workflows = _as_list(adv.get("workflows"))
    event_rules = _as_list(adv.get("event_rules") or adv.get("eventRules"))

    timed_pair_issue = False
    for job in jobs:
        if not isinstance(job, dict) or job.get("active", True) is False or _blank_adv_job(job):
            continue
        pair_id = _clean_text(job.get("pair_id"))
        if pair_id and pair_id not in enabled_pairs:
            timed_pair_issue = True
    if timed_pair_issue:
        warnings.append("Timed steps: Update the disabled or missing sync pair used by one or more timed steps.")

    workflow_pair_issue = False
    for workflow in workflows:
        if not isinstance(workflow, dict) or workflow.get("active", True) is False or _blank_workflow(workflow):
            continue
        steps = [step for step in _as_list(workflow.get("steps")) if isinstance(step, dict) and step.get("active", True) is not False]
        active_pair_steps = [step for step in steps if _clean_text(step.get("pair_id"))]
        if any(_clean_text(step.get("pair_id")) not in enabled_pairs for step in active_pair_steps):
            workflow_pair_issue = True
    if workflow_pair_issue:
        warnings.append("Recurring workflows: Update the disabled or missing sync pair used by one or more recurring workflows.")

    route_ids = _scrobble_event_route_ids(cfg)
    event_pair_issue = False
    event_bad_route = False
    for rule in event_rules:
        if not isinstance(rule, dict) or rule.get("active", True) is False:
            continue
        filters = _as_dict(rule.get("filters"))
        action = _as_dict(rule.get("action"))
        pair_id = _clean_text(action.get("pair_id") or action.get("pairId") or rule.get("pair_id"))
        route_id = _clean_text(filters.get("route_id") or filters.get("routeId"))
        if not pair_id and not route_id:
            continue
        source = _clean_text(rule.get("source")).lower()
        if route_id and source in route_ids and route_ids[source] and route_id not in route_ids[source]:
            event_bad_route = True
        if pair_id and pair_id not in enabled_pairs:
            event_pair_issue = True
    if event_bad_route:
        warnings.append("Event triggers: Choose a valid watcher or webhook route for one or more event triggers.")
    if event_pair_issue:
        warnings.append("Event triggers: Update the disabled or missing sync pair used by one or more event triggers.")

    return {"warning": bool(warnings), "warnings": _dedupe(warnings)}


def _scheduler_webhook_status(scfg: dict[str, Any]) -> dict[str, Any]:
    hooks = _as_dict(_as_dict(scfg).get("webhooks"))
    enabled = hooks.get("enabled") is True
    fmt_raw = _clean_text(hooks.get("payload_format") or hooks.get("payloadFormat") or hooks.get("format")).lower().replace("-", "_")
    notifiarr = fmt_raw in {"notifiarr", "notifiarr_passthrough"}
    label = "Notifiarr Passthrough" if notifiarr else "CrossWatch JSON"
    common_url = _clean_text(hooks.get("url") or hooks.get("default_url") or hooks.get("defaultUrl"))
    urls = [
        common_url,
        _clean_text(hooks.get("base_url") or hooks.get("healthchecks_base_url")),
        _clean_text(hooks.get("start_url")),
        _clean_text(hooks.get("success_url")),
        _clean_text(hooks.get("failure_url")),
    ]
    configured = bool(common_url) if notifiarr else any(urls)
    return {"enabled": enabled, "configured": configured, "label": label}


def _log_scheduler_webhook_status(prev_scfg: dict[str, Any], next_scfg: dict[str, Any], log: Callable[..., Any]) -> None:
    prev = _scheduler_webhook_status(prev_scfg)
    cur = _scheduler_webhook_status(next_scfg)
    if not cur["enabled"]:
        return
    if prev == cur:
        return
    suffix = f" ({cur['label']})" if cur.get("label") else ""
    message = f"Scheduler webhooks enabled{suffix}" if cur["configured"] else f"Scheduler webhooks enabled but no callback URL configured{suffix}"
    try:
        log("SYNC", "SCHED")(message, level="INFO")
    except Exception:
        pass


@router.post("/replan_now")
def replan_now() -> dict[str, Any]:
    load_config, _, scheduler, hint, compute_next, log = _env()
    cfg = load_config() or {}
    scfg = (cfg.get("scheduling") or {}) or {}
    effective_enabled = bool((scfg or {}).get("enabled") or ((scfg or {}).get("advanced") or {}).get("enabled"))

    try:
        nxt = int(compute_next(scfg) or 0)
    except Exception:
        nxt = 0

    now = int(time.time())
    try:
        hint["next_run_at"] = nxt
        hint["last_saved_at"] = now
    except Exception:
        pass

    try:
        if scheduler is not None:
            if effective_enabled:
                if hasattr(scheduler, "refresh"):
                    scheduler.refresh()
                elif hasattr(scheduler, "start"):
                    scheduler.start()
            elif hasattr(scheduler, "stop"):
                scheduler.stop()
    except Exception as e:
        try:
            log("SYNC", "SCHED")(f"replan_now worker refresh failed: {e}", level="ERROR")
        except Exception:
            pass

    try:
        st = scheduler.status()  # type: ignore[union-attr]
        st["config"] = scfg
        if not int(st.get("next_run_at") or 0):
            st["next_run_at"] = hint.get("next_run_at", nxt)
    except Exception:
        st = {"next_run_at": nxt, "config": scfg}

    return {"ok": True, **st}


@router.post("/trigger_now")
def trigger_now(payload: dict[str, Any] | None = Body(None)) -> dict[str, Any]:
    _, _, scheduler, _, _, log = _env()
    try:
        log("SYNC", "SCHED")("trigger_now: manual request received", level="INFO")
    except Exception:
        pass

    ok = False
    try:
        if scheduler is not None and hasattr(scheduler, "trigger_payload"):
            ok = bool(scheduler.trigger_payload(payload or None))
        elif scheduler is not None and hasattr(scheduler, "trigger_once"):
            scheduler.trigger_once()  # type: ignore[attr-defined]
            ok = True
    except Exception as e:
        try:
            log("SYNC", "SCHED")(f"trigger_now failed: {e}", level="ERROR")
        except Exception:
            pass
        return {"ok": False, "error": "trigger_failed"}

    try:
        st = scheduler.status()  # type: ignore[union-attr]
    except Exception:
        st = {}

    return {"ok": True, "triggered": ok, **st}


@router.post("/stop")
def sched_stop() -> dict[str, Any]:
    _, _, scheduler, hint, _, log = _env()
    try:
        log("SYNC", "SCHED")("stop: request received", level="INFO")
    except Exception:
        pass

    try:
        if scheduler is not None and hasattr(scheduler, "stop"):
            scheduler.stop()
    except Exception as e:
        try:
            log("SYNC", "SCHED")(f"stop failed: {e}", level="ERROR")
        except Exception:
            pass
        return {"ok": False, "error": "stop_failed"}

    try:
        if isinstance(hint, dict):
            hint["next_run_at"] = 0
    except Exception:
        pass

    try:
        st = scheduler.status()  # type: ignore[union-attr]
    except Exception:
        st = {}

    return {"ok": True, "stopped": True, **st}


@router.get("")
def sched_get() -> dict[str, Any]:
    load_config, *_ = _env()
    cfg = load_config() or {}
    return (cfg.get("scheduling") or {}) or {}

@router.post("")
def sched_post(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    load_config, save_config, scheduler, hint, compute_next, log = _env()
    cfg = load_config() or {}
    prev_scfg = _as_dict(cfg.get("scheduling"))
    cfg["scheduling"] = (payload or {}) or {}
    save_config(cfg)

    scfg = cfg["scheduling"] or {}
    _log_scheduler_webhook_status(prev_scfg, _as_dict(scfg), log)
    try:
        nxt = int(compute_next(scfg) or 0)
        hint["next_run_at"] = nxt
        hint["last_saved_at"] = int(time.time())
    except Exception:
        nxt = 0

    try:
        effective_enabled = bool((scfg or {}).get("enabled") or ((scfg or {}).get("advanced") or {}).get("enabled"))

        if effective_enabled:
            if hasattr(scheduler, "start"):
                scheduler.start()
            if hasattr(scheduler, "refresh"):
                scheduler.refresh()
        else:
            if hasattr(scheduler, "stop"):
                scheduler.stop()

        st = scheduler.status()  # type: ignore[union-attr]
        st["config"] = scfg
        return {"ok": True, "next_run_at": int(st.get("next_run_at") or nxt or 0)}
    except Exception:
        return {"ok": True, "next_run_at": int(nxt) if nxt else 0}

@router.get("/status")
def sched_status() -> dict[str, Any]:
    load_config, _, scheduler, hint, *_ = _env()
    try:
        st = scheduler.status()  # type: ignore[union-attr]
    except Exception:
        st = {}

    try:
        cfg = load_config() or {}
        st["config"] = (cfg.get("scheduling") or {}) or {}
        live = int(st.get("next_run_at") or 0)
        hint_val = int((hint.get("next_run_at") or 0)) if isinstance(hint, dict) else 0
        if not live and hint_val:
            st["next_run_at"] = hint_val
        warning_summary = _scheduling_warning_summary(cfg)
        st["scheduling_warnings"] = warning_summary["warnings"]
        st["warning"] = bool(warning_summary["warning"])
    except Exception:
        pass

    return st

@router.get("/next")
def sched_next() -> dict[str, Any]:
    load_config, _, _, _, compute_next, _ = _env()
    
    cfg = load_config() or {}
    scfg = (cfg.get("scheduling") or {}) or {}
    try:
        nxt = int(compute_next(scfg) or 0)
    except Exception:
        nxt = 0
    return {"ok": True, "next_run_at": nxt, "config": scfg}
