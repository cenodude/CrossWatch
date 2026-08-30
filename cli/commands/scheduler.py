# /cli/commands/scheduler.py
# CrossWatch - CLI scheduler commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import re
from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_NOT_FOUND, EXIT_USAGE, CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, error_text, find_pair, fmt_rel, fmt_ts, pair_label

scheduler_app = typer.Typer(help="Inspect and drive the sync scheduler.", no_args_is_help=True)

_HHMM_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_DAY_ALIASES = {
    "mon": 1,
    "monday": 1,
    "tue": 2,
    "tues": 2,
    "tuesday": 2,
    "wed": 3,
    "wednesday": 3,
    "thu": 4,
    "thur": 4,
    "thurs": 4,
    "thursday": 4,
    "fri": 5,
    "friday": 5,
    "sat": 6,
    "saturday": 6,
    "sun": 7,
    "sunday": 7,
}
_DAY_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def _status(state: Ctx) -> dict[str, Any]:
    payload = state.get("/api/scheduling/status")
    return as_dict(payload)


def _config(state: Ctx) -> dict[str, Any]:
    payload = state.get("/api/scheduling")
    return as_dict(payload)


def _pairs(state: Ctx) -> list[dict[str, Any]]:
    payload = state.get("/api/pairs")
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise CLIError(error_text(payload, "Cannot read pairs"))
    if not isinstance(payload, list):
        return []
    return [p for p in payload if isinstance(p, dict)]


def _save_config(state: Ctx, scfg: dict[str, Any]) -> dict[str, Any]:
    state.require_service("Changing the scheduler")
    payload = state.post("/api/scheduling", json_body=scfg)
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise CLIError(error_text(payload, "Scheduler update rejected"))
    return as_dict(payload)


def _advanced(scfg: dict[str, Any]) -> dict[str, Any]:
    adv = scfg.get("advanced")
    if not isinstance(adv, dict):
        adv = {}
        scfg["advanced"] = adv
    jobs = adv.get("jobs")
    if not isinstance(jobs, list):
        adv["jobs"] = []
    return adv


def _adv_jobs(scfg: dict[str, Any]) -> list[dict[str, Any]]:
    adv = _advanced(scfg)
    return [j for j in adv.get("jobs", []) if isinstance(j, dict)]


def _replace_adv_jobs(scfg: dict[str, Any], jobs: list[dict[str, Any]]) -> None:
    _advanced(scfg)["jobs"] = jobs


def _validate_time(value: str) -> str:
    text = str(value or "").strip()
    if not _HHMM_RE.fullmatch(text):
        raise CLIError(f"Invalid time '{value}'", hint="Use 24-hour HH:MM, for example 03:30.", exit_code=EXIT_USAGE)
    return text


def _parse_hours(value: str) -> int:
    text = str(value or "").strip().lower()
    match = re.fullmatch(r"(\d+)\s*h(?:ours?)?", text)
    if not match:
        match = re.fullmatch(r"(\d+)", text)
    if not match:
        raise CLIError(f"Invalid hour interval '{value}'", hint="Use a value like 6h or 6.", exit_code=EXIT_USAGE)
    hours = int(match.group(1))
    if hours < 1:
        raise CLIError("Hour interval must be at least 1.", exit_code=EXIT_USAGE)
    return hours


def _parse_minutes(value: str) -> int:
    text = str(value or "").strip().lower()
    match = re.fullmatch(r"(\d+)\s*m(?:in(?:utes?)?)?", text)
    if not match:
        match = re.fullmatch(r"(\d+)", text)
    if not match:
        raise CLIError(f"Invalid minute interval '{value}'", hint="Use a value like 45m or 45.", exit_code=EXIT_USAGE)
    minutes = int(match.group(1))
    if minutes < 15:
        raise CLIError("Custom interval must be at least 15 minutes.", exit_code=EXIT_USAGE)
    return minutes


def _parse_days(value: str) -> list[int]:
    text = str(value or "").strip().lower()
    if not text or text in {"all", "daily", "everyday", "every-day", "every day"}:
        return []
    if text in {"weekday", "weekdays"}:
        return [1, 2, 3, 4, 5]
    if text in {"weekend", "weekends"}:
        return [6, 7]
    out: list[int] = []
    for chunk in re.split(r"[, ]+", text):
        if not chunk:
            continue
        if re.fullmatch(r"[1-7]", chunk):
            day = int(chunk)
        elif chunk in _DAY_ALIASES:
            day = _DAY_ALIASES[chunk]
        else:
            raise CLIError(
                f"Unknown day '{chunk}'",
                hint="Use all, weekdays, weekends, or comma-separated names like mon,wed,fri.",
                exit_code=EXIT_USAGE,
            )
        if day not in out:
            out.append(day)
    return out


def _days_text(days: Any) -> str:
    if not isinstance(days, list) or not days:
        return "every day"
    labels = []
    for day in days:
        try:
            idx = int(day) - 1
        except Exception:
            continue
        if 0 <= idx < len(_DAY_NAMES):
            labels.append(_DAY_NAMES[idx])
    return ", ".join(labels) if labels else "every day"


def _slug(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower())
    return re.sub(r"_+", "_", text).strip("_") or "job"


def _unique_job_id(existing: list[dict[str, Any]], base: str) -> str:
    known = {str(j.get("id") or "") for j in existing}
    if base not in known:
        return base
    i = 2
    while f"{base}_{i}" in known:
        i += 1
    return f"{base}_{i}"


def _resolve_pair_selectors(pairs: list[dict[str, Any]], value: str) -> list[dict[str, Any]]:
    selectors = [part.strip() for part in str(value or "").split(",") if part.strip()]
    if not selectors:
        raise CLIError("At least one pair id or number is required", exit_code=EXIT_USAGE)
    resolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for selector in selectors:
        pair = find_pair(pairs, selector)
        pair_id = str(pair.get("id") or "")
        if pair_id in seen:
            continue
        seen.add(pair_id)
        resolved.append(pair)
    return resolved


def _append_pair_jobs(
    scfg: dict[str, Any],
    pairs: list[dict[str, Any]],
    times: list[str],
    *,
    days: str,
    after: str = "",
    paused: bool = False,
    job_id: str = "",
) -> list[dict[str, Any]]:
    if not pairs:
        raise CLIError("At least one pair is required", exit_code=EXIT_USAGE)
    if not times:
        raise CLIError("At least one --at time is required", hint="Example: cw scheduler add PAIR_ID --at 00:00 --at 12:00", exit_code=EXIT_USAGE)
    custom_id = job_id.strip()
    if custom_id and (len(pairs) > 1 or len(times) > 1):
        raise CLIError("--id can only be used when adding one job", exit_code=EXIT_USAGE)

    adv = _advanced(scfg)
    adv["enabled"] = True
    jobs = _adv_jobs(scfg)
    parsed_days = _parse_days(days)
    created: list[dict[str, Any]] = []
    for pair in pairs:
        resolved_pair_id = str(pair.get("id") or "")
        for time_text in times:
            base_id = custom_id or f"{_slug(resolved_pair_id)}_{time_text.replace(':', '')}"
            jid = _unique_job_id(jobs, base_id)
            job = {
                "id": jid,
                "pair_id": resolved_pair_id,
                "at": time_text,
                "days": parsed_days,
                "after": after.strip() or None,
                "active": not paused,
            }
            jobs.append(job)
            created.append(job)
    _replace_adv_jobs(scfg, jobs)
    return created


def _find_job(jobs: list[dict[str, Any]], needle: str) -> dict[str, Any]:
    wanted = str(needle or "").strip()
    if not wanted:
        raise CLIError("A job id is required", exit_code=EXIT_USAGE)
    exact = [j for j in jobs if str(j.get("id") or "") == wanted]
    if exact:
        return exact[0]
    lowered = wanted.lower()
    partial = [j for j in jobs if str(j.get("id") or "").lower().startswith(lowered)]
    if len(partial) == 1:
        return partial[0]
    if len(partial) > 1:
        ids = ", ".join(str(j.get("id") or "") for j in partial[:6])
        raise CLIError(f"Job id '{wanted}' is ambiguous", hint=f"Matches: {ids}", exit_code=EXIT_USAGE)
    raise CLIError(f"No scheduler job matches '{wanted}'", hint="Run 'cw scheduler list' to see jobs.", exit_code=EXIT_NOT_FOUND)


def _job_table_rows(scfg: dict[str, Any], pairs: list[dict[str, Any]]) -> list[list[Any]]:
    pair_by_id = {str(p.get("id") or ""): p for p in pairs}
    rows: list[list[Any]] = []
    for job in _adv_jobs(scfg):
        pair_id = str(job.get("pair_id") or "")
        pair = pair_by_id.get(pair_id)
        rows.append(
            [
                str(job.get("id") or ""),
                pair_label(pair) if pair else pair_id or "-",
                str(job.get("at") or "-"),
                _days_text(job.get("days")),
                state_text(job.get("active", True) is not False, on="active", off="paused"),
                str(job.get("after") or "-"),
            ]
        )
    return rows


def _print_preview(state: Ctx, scfg: dict[str, Any], *, title: str = "Scheduler change") -> None:
    if state.out.json_mode:
        state.out.data({"dry_run": True, "scheduling": scfg})
        return
    state.out.kv(
        [
            ("Standard enabled", state_text(coerce_bool(scfg.get("enabled"), False), on="yes", off="no")),
            ("Mode", _mode(scfg)),
            ("Advanced enabled", state_text(coerce_bool(as_dict(scfg.get("advanced")).get("enabled"), False), on="yes", off="no")),
            ("Advanced jobs", str(len(_adv_jobs(scfg)))),
        ],
        title=title,
    )


def _finish_save(state: Ctx, scfg: dict[str, Any], *, dry_run: bool, message: str) -> None:
    if dry_run:
        _print_preview(state, scfg)
        return
    payload = _save_config(state, scfg)
    nxt = int(payload.get("next_run_at") or 0)
    if state.out.json_mode:
        state.out.data({"ok": True, "next_run_at": nxt, "scheduling": scfg})
        return
    state.out.success(message)
    if nxt:
        state.out.info(f"Next run {fmt_ts(nxt)} ({fmt_rel(nxt)})")


def _mode(scfg: dict[str, Any]) -> str:
    advanced = as_dict(scfg.get("advanced"))
    if coerce_bool(advanced.get("enabled"), False):
        return "advanced"
    mode = str(scfg.get("mode") or "").strip()
    if mode:
        return mode
    every = scfg.get("every_n_hours") or scfg.get("interval_hours")
    return f"every {every}h" if every else "simple"


def _render(state: Ctx, status: dict[str, Any]) -> None:
    if state.out.json_mode:
        state.out.data(status)
        return
    scfg = as_dict(status.get("config"))
    advanced = as_dict(scfg.get("advanced"))
    standard_enabled = coerce_bool(scfg.get("enabled"), False)
    advanced_enabled = coerce_bool(advanced.get("enabled"), False)
    nxt = int(status.get("next_run_at") or 0)
    warnings = status.get("scheduling_warnings") or []
    state.out.kv(
        [
            ("Enabled", state_text(standard_enabled or advanced_enabled, on="yes", off="no")),
            ("Standard", state_text(standard_enabled, on="on", off="off")),
            ("Advanced", state_text(advanced_enabled, on="on", off="off")),
            ("Worker", state_text(coerce_bool(status.get("running"), False), on="running", off="stopped")),
            ("Mode", _mode(scfg)),
            ("Next run", f"{fmt_ts(nxt)}  ({fmt_rel(nxt)})" if nxt else "not scheduled"),
            ("Last run", fmt_ts(status.get("last_run_at")) if status.get("last_run_at") else "-"),
            ("Warnings", str(len(warnings)) if warnings else "none"),
        ],
        title="Scheduler",
    )
    if warnings:
        state.out.print()
        rows = []
        for item in warnings:
            if isinstance(item, dict):
                rows.append([str(item.get("code") or item.get("id") or "-"), str(item.get("message") or item.get("text") or "-")])
            else:
                rows.append(["-", str(item)])
        state.out.table(["CODE", "WARNING"], rows, title="Scheduling warnings")


@scheduler_app.command("status")
def scheduler_status(ctx: typer.Context) -> None:
    """Show scheduler state and the next planned run."""
    state: Ctx = ctx.obj
    _render(state, _status(state))


@scheduler_app.command("next")
def scheduler_next(ctx: typer.Context) -> None:
    """Show only the next planned run."""
    state: Ctx = ctx.obj
    payload = state.get("/api/scheduling/next")
    nxt = int((payload or {}).get("next_run_at") or 0)
    if state.out.json_mode:
        state.out.data(as_dict(payload))
        return
    if not nxt:
        state.out.info("No run is scheduled.")
        return
    state.out.raw(f"{fmt_ts(nxt)}  ({fmt_rel(nxt)})")


@scheduler_app.command("list")
def scheduler_list(ctx: typer.Context) -> None:
    """List advanced scheduler jobs."""
    state: Ctx = ctx.obj
    scfg = _config(state)
    pairs = _pairs(state)
    if state.out.json_mode:
        state.out.data({"advanced_enabled": coerce_bool(as_dict(scfg.get("advanced")).get("enabled"), False), "jobs": _adv_jobs(scfg)})
        return
    state.out.table(
        ["ID", "PAIR", "TIME", "DAYS", "STATE", "AFTER"],
        _job_table_rows(scfg, pairs),
        title="Advanced scheduler jobs",
        empty="No advanced scheduler jobs configured.",
    )


@scheduler_app.command("set")
def scheduler_set(
    ctx: typer.Context,
    mode: str = typer.Argument(..., help="hourly, every, daily, or interval."),
    value: str = typer.Argument("", help="Interval or time, for example 6h, 03:30, or 45m."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview the change without saving."),
) -> None:
    """Configure the standard scheduler without editing JSON."""
    state: Ctx = ctx.obj
    scfg = copy.deepcopy(_config(state))
    wanted = str(mode or "").strip().lower().replace("-", "_")
    if wanted in {"hourly", "hour"}:
        scfg["enabled"] = True
        scfg["mode"] = "hourly"
        scfg["every_n_hours"] = 1
    elif wanted in {"every", "every_n_hours"}:
        hours = _parse_hours(value)
        scfg["enabled"] = True
        scfg["mode"] = "hourly" if hours == 1 else "every_n_hours"
        scfg["every_n_hours"] = hours
    elif wanted in {"daily", "daily_time", "at"}:
        scfg["enabled"] = True
        scfg["mode"] = "daily_time"
        scfg["daily_time"] = _validate_time(value)
    elif wanted in {"interval", "custom", "custom_interval"}:
        scfg["enabled"] = True
        scfg["mode"] = "custom_interval"
        scfg["custom_interval_minutes"] = _parse_minutes(value)
    else:
        raise CLIError(f"Unknown scheduler mode '{mode}'", hint="Use hourly, every, daily, or interval.", exit_code=EXIT_USAGE)

    _advanced(scfg)["enabled"] = False
    _finish_save(state, scfg, dry_run=dry_run, message="Standard scheduler configured.")


@scheduler_app.command("run-now")
def scheduler_run_now(ctx: typer.Context) -> None:
    """Ask the scheduler to fire its job immediately."""
    state: Ctx = ctx.obj
    state.require_service("Triggering the scheduler")
    payload = state.post("/api/scheduling/trigger_now")
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise CLIError(error_text(payload, "Trigger failed"))
    if state.out.json_mode:
        state.out.data(payload if isinstance(payload, dict) else {"ok": True})
        return
    if isinstance(payload, dict) and payload.get("triggered") is False:
        state.out.warn("Scheduler accepted the request but did not fire; is it enabled?")
        return
    state.out.success("Scheduler fired.")


@scheduler_app.command("replan")
def scheduler_replan(ctx: typer.Context) -> None:
    """Recompute the next run time from the current configuration."""
    state: Ctx = ctx.obj
    state.require_service("Replanning the scheduler")
    payload = state.post("/api/scheduling/replan_now")
    if not state.out.json_mode:
        state.out.success("Replanned.")
        state.out.print()
    _render(state, as_dict(payload))


@scheduler_app.command("enable")
def scheduler_enable(ctx: typer.Context) -> None:
    """Turn the scheduler on."""
    _set_enabled(ctx, True)


@scheduler_app.command("disable")
def scheduler_disable(ctx: typer.Context) -> None:
    """Turn the scheduler off."""
    _set_enabled(ctx, False)


def _set_enabled(ctx: typer.Context, enabled: bool) -> None:
    state: Ctx = ctx.obj
    state.require_service("Changing the scheduler")
    scfg = dict(_config(state))
    scfg["enabled"] = bool(enabled)
    payload = state.post("/api/scheduling", json_body=scfg)
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise CLIError(error_text(payload, "Scheduler update rejected"))
    nxt = int((payload or {}).get("next_run_at") or 0)
    if state.out.json_mode:
        state.out.data({"ok": True, "enabled": enabled, "next_run_at": nxt})
        return
    state.out.success(f"Scheduler {'enabled' if enabled else 'disabled'}.")
    if enabled and nxt:
        state.out.info(f"Next run {fmt_ts(nxt)} ({fmt_rel(nxt)})")


@scheduler_app.command("add")
def scheduler_add(
    ctx: typer.Context,
    pair_id: str = typer.Argument(..., help="Sync pair id, prefix, label, or list number."),
    at: list[str] = typer.Option([], "--at", "-t", help="Run time in HH:MM. Repeat for multiple daily runs."),
    days: str = typer.Option("all", "--days", help="all, weekdays, weekends, or names like mon,wed,fri."),
    job_id: str = typer.Option("", "--id", help="Job id. Only allowed with one --at."),
    after: str = typer.Option("", "--after", help="Run after another due job id."),
    paused: bool = typer.Option(False, "--paused", help="Create the job paused."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview the change without saving."),
) -> None:
    """Add one or more advanced pair jobs."""
    state: Ctx = ctx.obj
    if not at:
        raise CLIError("At least one --at time is required", hint="Example: cw scheduler add PAIR_ID --at 00:00 --at 12:00", exit_code=EXIT_USAGE)
    times = [_validate_time(item) for item in at]

    pair = find_pair(_pairs(state), pair_id)
    scfg = copy.deepcopy(_config(state))
    created = _append_pair_jobs(scfg, [pair], times, days=days, after=after, paused=paused, job_id=job_id)

    if dry_run:
        _print_preview(state, scfg, title="Scheduler add preview")
        return
    payload = _save_config(state, scfg)
    if state.out.json_mode:
        state.out.data({"ok": True, "created": created, "next_run_at": int(payload.get("next_run_at") or 0)})
        return
    state.out.success(f"Added {len(created)} advanced scheduler job(s) for {pair_label(pair)}.")
    nxt = int(payload.get("next_run_at") or 0)
    if nxt:
        state.out.info(f"Next run {fmt_ts(nxt)} ({fmt_rel(nxt)})")


@scheduler_app.command("edit")
def scheduler_edit(
    ctx: typer.Context,
    job_id: str = typer.Argument(..., help="Job id or prefix."),
    pair_id: str = typer.Option("", "--pair", help="Change the sync pair."),
    at: str = typer.Option("", "--at", "-t", help="Change run time in HH:MM."),
    days: str | None = typer.Option(None, "--days", help="Change days: all, weekdays, weekends, or mon,wed,fri."),
    after: str | None = typer.Option(None, "--after", help="Change dependency. Pass an empty value to clear."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview the change without saving."),
) -> None:
    """Edit an advanced pair job."""
    state: Ctx = ctx.obj
    scfg = copy.deepcopy(_config(state))
    jobs = _adv_jobs(scfg)
    job = _find_job(jobs, job_id)
    changed = False

    if pair_id.strip():
        pair = find_pair(_pairs(state), pair_id)
        job["pair_id"] = str(pair.get("id") or "")
        changed = True
    if at.strip():
        job["at"] = _validate_time(at)
        changed = True
    if days is not None:
        job["days"] = _parse_days(days)
        changed = True
    if after is not None:
        job["after"] = str(after or "").strip() or None
        changed = True
    if not changed:
        raise CLIError("Nothing to edit", hint="Pass --pair, --at, --days, or --after.", exit_code=EXIT_USAGE)

    _advanced(scfg)["enabled"] = True
    _replace_adv_jobs(scfg, jobs)
    _finish_save(state, scfg, dry_run=dry_run, message=f"Updated scheduler job {job['id']}.")


@scheduler_app.command("pause")
def scheduler_pause(
    ctx: typer.Context,
    job_id: str = typer.Argument(..., help="Job id or prefix."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview the change without saving."),
) -> None:
    """Pause an advanced scheduler job."""
    _set_job_active(ctx, job_id, False, dry_run=dry_run)


@scheduler_app.command("resume")
def scheduler_resume(
    ctx: typer.Context,
    job_id: str = typer.Argument(..., help="Job id or prefix."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview the change without saving."),
) -> None:
    """Resume an advanced scheduler job."""
    _set_job_active(ctx, job_id, True, dry_run=dry_run)


def _set_job_active(ctx: typer.Context, job_id: str, active: bool, *, dry_run: bool) -> None:
    state: Ctx = ctx.obj
    scfg = copy.deepcopy(_config(state))
    jobs = _adv_jobs(scfg)
    job = _find_job(jobs, job_id)
    job["active"] = bool(active)
    _replace_adv_jobs(scfg, jobs)
    _finish_save(state, scfg, dry_run=dry_run, message=f"{'Resumed' if active else 'Paused'} scheduler job {job['id']}.")


@scheduler_app.command("delete")
def scheduler_delete(
    ctx: typer.Context,
    job_id: str = typer.Argument(..., help="Job id or prefix."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview the change without saving."),
) -> None:
    """Delete an advanced scheduler job."""
    state: Ctx = ctx.obj
    scfg = copy.deepcopy(_config(state))
    jobs = _adv_jobs(scfg)
    job = _find_job(jobs, job_id)
    if not yes and not dry_run:
        state.out.print(f"About to delete scheduler job [cw.accent]{job.get('id')}[/]")
        if not typer.confirm("Delete this job?", default=False):
            raise CLIError("Cancelled", exit_code=0)
    remaining = [j for j in jobs if j is not job]
    _replace_adv_jobs(scfg, remaining)
    _finish_save(state, scfg, dry_run=dry_run, message=f"Deleted scheduler job {job['id']}.")


@scheduler_app.command("setup")
def scheduler_setup(
    ctx: typer.Context,
    kind: str = typer.Option("", "--kind", help="standard or pair."),
    pair_id: str = typer.Option("", "--pair", help="Pair id/prefix/number for pair scheduling. Use commas for multiple pairs."),
    cadence: str = typer.Option("", "--cadence", help="hourly, every, daily, or interval."),
    value: str = typer.Option("", "--value", help="Interval or daily time, for example 6h, 03:30, or 45m."),
    at: list[str] = typer.Option([], "--at", "-t", help="Run time in HH:MM. Repeat for multiple daily runs."),
    days: str = typer.Option("all", "--days", help="all, weekdays, weekends, or names like mon,wed,fri."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Save without confirmation."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview the change without saving."),
) -> None:
    """Guided scheduler setup that chooses standard or advanced config."""
    state: Ctx = ctx.obj
    chosen_kind = str(kind or "").strip().lower()
    if not chosen_kind:
        state.out.print("What do you want to schedule?")
        state.out.print("  1. All enabled sync pairs on one cadence")
        state.out.print("  2. One sync pair at specific time(s)")
        chosen_kind = str(typer.prompt("Choose", default="1") or "1").strip().lower()
    if chosen_kind in {"1", "all", "standard", "all-pairs", "all_pairs"}:
        _setup_standard(ctx, cadence=cadence, value=value, yes=yes, dry_run=dry_run)
        return
    if chosen_kind not in {"2", "pair", "advanced", "one-pair", "one_pair", "multi-time", "multi_time"}:
        raise CLIError("This setup path is not implemented yet", hint="Use --kind standard or --kind pair.", exit_code=EXIT_USAGE)

    pairs = _pairs(state)
    selected_pair = pair_id.strip()
    if not selected_pair:
        state.out.table(
            ["#", "ID", "PAIR"],
            [[idx, str(p.get("id") or ""), pair_label(p)] for idx, p in enumerate(pairs, start=1)],
            title="Sync pairs",
            empty="No sync pairs configured yet.",
        )
        selected_pair = str(typer.prompt("Pair id(s) or number(s), comma-separated") or "").strip()
    selected_pairs = _resolve_pair_selectors(pairs, selected_pair)
    chosen_at = list(at)
    if not chosen_at:
        raw = str(typer.prompt("Run time(s), comma-separated HH:MM", default="03:30") or "").strip()
        chosen_at = [part.strip() for part in raw.split(",") if part.strip()]
    chosen_times = [_validate_time(item) for item in chosen_at]
    if not yes and not dry_run:
        state.out.info(f"Will create {len(selected_pairs) * len(chosen_times)} advanced job(s) for {len(selected_pairs)} sync pair(s).")
        if not typer.confirm("Save and enable advanced scheduler?", default=True):
            raise CLIError("Cancelled", exit_code=0)
    scfg = copy.deepcopy(_config(state))
    created = _append_pair_jobs(scfg, selected_pairs, chosen_times, days=days)
    if dry_run:
        _print_preview(state, scfg, title="Scheduler setup preview")
        return
    payload = _save_config(state, scfg)
    if state.out.json_mode:
        state.out.data({"ok": True, "created": created, "next_run_at": int(payload.get("next_run_at") or 0)})
        return
    pair_word = "pair" if len(selected_pairs) == 1 else "pairs"
    state.out.success(f"Added {len(created)} advanced scheduler job(s) for {len(selected_pairs)} sync {pair_word}.")
    nxt = int(payload.get("next_run_at") or 0)
    if nxt:
        state.out.info(f"Next run {fmt_ts(nxt)} ({fmt_rel(nxt)})")


def _setup_standard(ctx: typer.Context, *, cadence: str, value: str, yes: bool, dry_run: bool) -> None:
    state: Ctx = ctx.obj
    chosen = str(cadence or "").strip().lower()
    if not chosen:
        state.out.print("How often should all enabled pairs run?")
        state.out.print("  1. Hourly")
        state.out.print("  2. Every N hours")
        state.out.print("  3. Daily at a time")
        state.out.print("  4. Custom interval in minutes")
        chosen = str(typer.prompt("Choose", default="2") or "2").strip().lower()
    if chosen in {"1", "hourly"}:
        mode, val = "hourly", ""
    elif chosen in {"2", "every", "every_n_hours"}:
        mode = "every"
        val = value.strip() or str(typer.prompt("Every how many hours?", default="12") or "12")
    elif chosen in {"3", "daily", "daily_time"}:
        mode = "daily"
        val = value.strip() or str(typer.prompt("Daily time", default="03:30") or "03:30")
    elif chosen in {"4", "interval", "custom", "custom_interval"}:
        mode = "interval"
        val = value.strip() or str(typer.prompt("Interval minutes", default="60") or "60")
    else:
        raise CLIError(f"Unknown cadence '{chosen}'", exit_code=EXIT_USAGE)

    if not yes and not dry_run:
        state.out.info(f"Will configure the standard scheduler: {mode} {val}".strip())
        if not typer.confirm("Save and enable scheduler?", default=True):
            raise CLIError("Cancelled", exit_code=0)
    scheduler_set(ctx, mode, val, dry_run=dry_run)


@scheduler_app.command("stop")
def scheduler_stop(ctx: typer.Context) -> None:
    """Stop the scheduler worker without changing the saved configuration."""
    state: Ctx = ctx.obj
    state.require_service("Stopping the scheduler")
    payload = state.post("/api/scheduling/stop")
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise CLIError(error_text(payload, "Stop failed"))
    if state.out.json_mode:
        state.out.data(payload if isinstance(payload, dict) else {"ok": True})
        return
    state.out.success("Scheduler worker stopped.")


@scheduler_app.command("show")
def scheduler_show(ctx: typer.Context) -> None:
    """Print the raw scheduling configuration."""
    state: Ctx = ctx.obj
    scfg = _config(state)
    if state.out.json_mode:
        state.out.data(scfg)
        return
    import json as _json

    from rich.syntax import Syntax

    state.out.console.print(Syntax(_json.dumps(scfg, indent=2, default=str), "json", theme="ansi_dark", background_color="default"))


def register(app: typer.Typer) -> None:
    app.add_typer(scheduler_app, name="scheduler")
