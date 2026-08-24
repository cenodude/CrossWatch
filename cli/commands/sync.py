# /cli/commands/sync.py
# CrossWatch - CLI sync run commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import queue
import re
import threading
import time
from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_BUSY, CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, error_text, find_pair, fmt_duration, fmt_ts, is_log_control, pair_features, pair_label, parse_iso, strip_ansi

sync_app = typer.Typer(help="Trigger, watch and inspect sync runs.", no_args_is_help=True)

EXIT_LINE = re.compile(r"\[SYNC\]\s*exit code:\s*(-?\d+)")
LOG_TAG = "SYNC"
COUNT_FEATURES = ("watchlist", "ratings", "history", "progress", "playlists", "collection")
COUNT_HEADERS = ("WATCHLIST", "RATINGS", "HISTORY", "PROGRESS", "PLAYLISTS", "COLLECTION")


def _summary(state: Ctx) -> dict[str, Any]:
    payload = state.get("/api/run/summary")
    return as_dict(payload)


def _run_state(state: Ctx) -> dict[str, Any]:
    payload = state.get("/api/run/cancel")
    return as_dict(payload)


def _pairs(state: Ctx) -> list[dict[str, Any]]:
    payload = state.get("/api/pairs")
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise CLIError(error_text(payload, "Cannot read pairs"))
    if not isinstance(payload, list):
        return []
    return [p for p in payload if isinstance(p, dict)]


def _provider_ref(pair: dict[str, Any], side: str) -> str:
    provider = str(pair.get(side) or "?").strip().upper()
    instance = str(pair.get(f"{side}_instance") or "default").strip()
    if not instance or instance.lower() == "default":
        return provider
    return f"{provider}:{instance}"


def _feature_summary(pair: dict[str, Any]) -> str:
    return ", ".join(pair_features(pair)) or "-"


class _Follower:
    def __init__(self, state: Ctx) -> None:
        self.state = state
        self.inbox: "queue.Queue[tuple[str, str]]" = queue.Queue()
        self.stop = threading.Event()
        self.failure: list[Exception] = []
        self.client = state.stream_client()
        self.worker = threading.Thread(target=self._pump, name="cw-log-follow", daemon=True)

    def start(self, settle: float = 0.35) -> "_Follower":
        self.worker.start()
        time.sleep(settle)
        return self

    def _pump(self) -> None:
        try:
            for event, data in self.client.stream_sse(
                "/api/logs/stream", params={"tag": LOG_TAG, "plain": "true", "tail": 1}
            ):
                if self.stop.is_set():
                    break
                self.inbox.put((event, data))
        except Exception as exc:
            self.failure.append(exc)
        finally:
            self.inbox.put(("__closed__", ""))

    def close(self) -> None:
        self.stop.set()
        try:
            self.client.close()
        except Exception:
            pass

    def drain(self, *, timeout: float) -> int:
        state = self.state
        deadline = time.time() + timeout if timeout > 0 else 0.0
        exit_code = 0
        idle_checks = 0
        try:
            while True:
                try:
                    event, data = self.inbox.get(timeout=1.0)
                except queue.Empty:
                    if deadline and time.time() > deadline:
                        state.out.warn(f"Stopped following after {fmt_duration(timeout)}; the run continues in the background.")
                        return EXIT_BUSY
                    idle_checks += 1
                    if idle_checks % 5 == 0 and not coerce_bool(_run_state(state).get("running"), False):
                        return exit_code
                    continue
                idle_checks = 0
                if event == "__closed__":
                    if self.failure:
                        raise self.failure[0]
                    return exit_code
                if event in ("ping", "scope"):
                    continue
                line = strip_ansi(data)
                if not line.strip() or is_log_control(line):
                    continue
                state.out.raw(line)
                match = EXIT_LINE.search(line)
                if match:
                    return int(match.group(1))
                if deadline and time.time() > deadline:
                    state.out.warn(f"Stopped following after {fmt_duration(timeout)}; the run continues in the background.")
                    return EXIT_BUSY
        finally:
            self.close()


@sync_app.command("run")
def sync_run(
    ctx: typer.Context,
    target: str = typer.Argument("", help="Optional pair number, id, prefix or label. Default runs every enabled pair."),
    pair: str = typer.Option("", "--pair", "-p", help="Run one pair only (number, id, prefix or label)."),
    follow: bool = typer.Option(False, "--follow", "-f", help="Stream the sync log until the run finishes."),
    timeout: float = typer.Option(0.0, "--timeout", help="Give up following after this many seconds (0 = no limit)."),
) -> None:
    """Start a sync run."""
    state: Ctx = ctx.obj
    state.require_service("Starting a sync")
    payload: dict[str, Any] = {"source": "cli"}
    label = "all enabled pairs"
    selector = (target or "").strip() or (pair or "").strip()
    if selector:
        selected_pair = find_pair(
            _pairs(state),
            selector,
        )
        payload["pair_id"] = str(selected_pair.get("id") or "")
        label = f"{pair_label(selected_pair)} ({selected_pair.get('id')})"

    if follow:
        code = _start_and_follow(state, payload, label, timeout)
        raise typer.Exit(code)

    result = state.post("/api/run", json_body=payload)
    if isinstance(result, dict) and result.get("ok") is False:
        error = error_text(result, "Sync refused")
        raise CLIError(error, exit_code=EXIT_BUSY if "already running" in error.lower() else 1)
    if state.out.json_mode:
        state.out.data(result if isinstance(result, dict) else {"ok": True})
        return
    if isinstance(result, dict) and result.get("skipped"):
        state.out.warn(f"Nothing to do: {result.get('skipped')}")
        return
    run_id = str((result or {}).get("run_id") or "")
    state.out.success(f"Sync started for {label}" + (f" (run {run_id})" if run_id else ""))
    state.out.info("Follow it with 'cw logs tail -f' or 'cw sync status'.")


@sync_app.command("list")
def sync_list(
    ctx: typer.Context,
    enabled_only: bool = typer.Option(False, "--enabled", help="Only show enabled pairs."),
) -> None:
    """List configured sync pairs."""
    state: Ctx = ctx.obj
    pairs = _pairs(state)
    if enabled_only:
        pairs = [p for p in pairs if p.get("enabled", True) is not False]
    if state.out.json_mode:
        state.out.data(pairs)
        return
    state.out.table(
        ["#", "ID", "SOURCE", "TARGET", "MODE", "STATE", "FEATURES"],
        [
            [
                str(index),
                str(p.get("id") or ""),
                _provider_ref(p, "source"),
                _provider_ref(p, "target"),
                str(p.get("mode") or "one-way"),
                state_text(p.get("enabled", True) is not False, on="enabled", off="disabled"),
                _feature_summary(p),
            ]
            for index, p in enumerate(pairs, start=1)
        ],
        title="Sync pairs",
        empty="No pairs configured yet.",
    )


def _start_and_follow(state: Ctx, payload: dict[str, Any], label: str, timeout: float) -> int:
    follower = _Follower(state).start()
    try:
        result = state.post("/api/run", json_body=payload)
    except Exception:
        follower.close()
        raise
    if isinstance(result, dict) and result.get("ok") is False:
        follower.close()
        error = error_text(result, "Sync refused")
        raise CLIError(error, exit_code=EXIT_BUSY if "already running" in error.lower() else 1)
    if isinstance(result, dict) and result.get("skipped"):
        follower.close()
        state.out.warn(f"Nothing to do: {result.get('skipped')}")
        return 0

    state.out.info(f"Running {label} (run {(result or {}).get('run_id') or '?'})")
    return follower.drain(timeout=timeout)


@sync_app.command("status")
def sync_status(ctx: typer.Context) -> None:
    """Show the current or last sync run."""
    state: Ctx = ctx.obj
    run = _run_state(state)
    summary = _summary(state)
    running = coerce_bool(run.get("running"), False)
    started = parse_iso(summary.get("started_at")) or int(float(summary.get("raw_started_ts") or 0) or 0)
    if state.out.json_mode:
        state.out.data({"running": running, "run": run, "summary": summary})
        return
    timeline = as_dict(summary.get("timeline"))
    phases = [name for name in ("start", "pre", "post", "done") if timeline.get(name)]
    state.out.kv(
        [
            ("State", state_text(running, on="running", off="idle")),
            ("Run id", str(summary.get("run_id") or run.get("run_id") or "-")),
            ("Started", fmt_ts(started)),
            ("Finished", fmt_ts(parse_iso(summary.get("finished_at")))),
            ("Duration", fmt_duration(summary.get("duration_sec")) if summary.get("duration_sec") else "-"),
            ("Result", str(summary.get("result") or "-")),
            ("Exit code", str(summary.get("exit_code")) if summary.get("exit_code") is not None else "-"),
            ("Phases", " > ".join(phases) or "-"),
            ("Cancel requested", state_text(coerce_bool(run.get("cancel_requested"), False), on="yes", off="no")),
            ("Added", str(summary.get("added_last") or 0)),
            ("Updated", str(summary.get("updated_last") or 0)),
            ("Removed", str(summary.get("removed_last") or 0)),
        ],
        title="Sync run",
    )

    counts = summary.get("provider_counts")
    if isinstance(counts, dict) and counts:
        rows = []
        for name, block in sorted(counts.items()):
            if isinstance(block, dict):
                rows.append([name, *[str(block.get(k, "-")) for k in COUNT_FEATURES]])
        if rows:
            state.out.print()
            state.out.table(["PROVIDER", *COUNT_HEADERS], rows, title="Provider counts")


@sync_app.command("follow")
def sync_follow(
    ctx: typer.Context,
    timeout: float = typer.Option(0.0, "--timeout", help="Give up after this many seconds (0 = no limit)."),
) -> None:
    """Attach to the sync log of a run that is already going."""
    state: Ctx = ctx.obj
    state.require_service("Following a sync")
    if not coerce_bool(_run_state(state).get("running"), False):
        state.out.warn("No sync is running right now; showing new output as it appears.")
    raise typer.Exit(_Follower(state).start(0.0).drain(timeout=timeout))


@sync_app.command("cancel")
def sync_cancel(ctx: typer.Context) -> None:
    """Ask the running sync to stop after the current step."""
    state: Ctx = ctx.obj
    state.require_service("Cancelling a sync")
    result = state.post("/api/run/cancel")
    if state.out.json_mode:
        state.out.data(result if isinstance(result, dict) else {"ok": True})
        return
    if isinstance(result, dict) and result.get("ok") is False:
        state.out.warn(error_text(result, "Nothing to cancel"))
        return
    state.out.success("Cancel requested; the run stops after the current step.")


@sync_app.command("unresolved")
def sync_unresolved(
    ctx: typer.Context,
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows to show."),
) -> None:
    """List items the last run could not match."""
    state: Ctx = ctx.obj
    payload = state.get("/api/run/unresolved")
    items = payload.get("items") if isinstance(payload, dict) else []
    items = [i for i in (items or []) if isinstance(i, dict)]
    if state.out.json_mode:
        state.out.data({"total": (payload or {}).get("total", len(items)), "items": items})
        return
    state.out.table(
        ["TITLE", "TYPE", "YEAR", "REASON"],
        [
            [
                str(i.get("title") or i.get("line") or "-")[:60],
                str(i.get("type") or "-"),
                str(i.get("year") or "-"),
                str(i.get("reason") or "-")[:40],
            ]
            for i in items[: max(1, limit)]
        ],
        title=f"Unresolved items ({(payload or {}).get('total', len(items))})",
        empty="Nothing unresolved.",
    )


@sync_app.command("providers")
def sync_providers(
    ctx: typer.Context,
    counts: bool = typer.Option(False, "--counts", help="Show per-provider item counts instead of capabilities."),
) -> None:
    """Show providers the sync engine can use."""
    state: Ctx = ctx.obj
    path = "/api/sync/providers/counts" if counts else "/api/sync/providers"
    payload = state.get(path)
    if state.out.json_mode:
        state.out.data(payload)
        return
    if counts:
        source = payload.get("providers") if isinstance(payload, dict) else payload
        rows = []
        if isinstance(source, dict):
            for name, block in sorted(source.items()):
                if isinstance(block, dict):
                    rows.append([name, *[str(block.get(k, "-")) for k in COUNT_FEATURES]])
        state.out.table(["PROVIDER", *COUNT_HEADERS], rows, title="Provider counts")
        return
    source = payload.get("providers") if isinstance(payload, dict) else payload
    rows = []
    if isinstance(source, list):
        for entry in source:
            if isinstance(entry, dict):
                features = as_dict(entry.get("features"))
                rows.append(
                    [
                        str(entry.get("name") or entry.get("id") or "-"),
                        str(entry.get("label") or "-"),
                        ", ".join(sorted(k for k, v in features.items() if v)) or "-",
                    ]
                )
            else:
                rows.append([str(entry), "-", "-"])
    elif isinstance(source, dict):
        for name, entry in sorted(source.items()):
            rows.append([name, str((entry or {}).get("label") or "-") if isinstance(entry, dict) else "-", "-"])
    state.out.table(["PROVIDER", "LABEL", "FEATURES"], rows, title="Sync providers")


def register(app: typer.Typer) -> None:
    app.add_typer(sync_app, name="sync")
