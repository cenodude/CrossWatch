# /cli/commands/analyzer.py
# CrossWatch - CLI analyzer commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._util import as_dict, error_text, rows_from_payload

analyzer_app = typer.Typer(help="Find items that are stuck or inconsistent between providers.", no_args_is_help=True)


def _scope(pairs: str) -> dict[str, Any] | None:
    return {"pairs": pairs.strip()} if pairs.strip() else None


def _items(payload: Any, *keys: str) -> list[dict[str, Any]]:
    return rows_from_payload(payload, *keys)


def _item_title(row: dict[str, Any]) -> str:
    item_type = str(row.get("item_type") or row.get("type") or row.get("type_name") or "").lower()
    series = str(row.get("series_title") or "").strip()
    title = str(row.get("title") or row.get("item_title") or "").strip()
    season = row.get("season")
    episode = row.get("episode")
    if item_type == "episode" and series and season is not None and episode is not None:
        try:
            return f"{series} - S{int(season):02d}E{int(episode):02d}"
        except Exception:
            return f"{series} - S{season}E{episode}"
    if item_type == "season" and series and season is not None:
        try:
            return f"{series} - S{int(season):02d}"
        except Exception:
            return f"{series} - S{season}"
    return title or str(row.get("key") or "-")


def _problem_detail(row: dict[str, Any]) -> str:
    message = str(row.get("message") or row.get("text") or "").strip()
    if message:
        return message
    targets = [str(t or "").upper() for t in row.get("targets") or [] if str(t or "").strip()]
    if targets:
        return "Missing at " + " & ".join(targets)
    for detail in row.get("target_show_info") or []:
        if isinstance(detail, dict) and str(detail.get("message") or "").strip():
            return str(detail.get("message")).strip()
    for hint in row.get("hints") or []:
        if not isinstance(hint, dict):
            continue
        if str(hint.get("message") or "").strip():
            return str(hint.get("message")).strip()
        reasons = hint.get("reasons")
        if isinstance(reasons, list) and reasons:
            return ", ".join(str(r) for r in reasons if str(r).strip())
        if str(hint.get("reason") or "").strip():
            return str(hint.get("reason")).strip()
    return str(row.get("code") or row.get("id") or row.get("type") or "-")


@analyzer_app.command("problems")
def analyzer_problems(
    ctx: typer.Context,
    pairs: str = typer.Option("", "--pairs", "-p", help="Limit to these pair ids, comma separated."),
    system: bool = typer.Option(False, "--system", help="Include system level problems."),
    hints: bool = typer.Option(False, "--hints", help="Include hints."),
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows."),
) -> None:
    """List what the analyzer thinks is wrong."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {"include_system": system, "include_hints": hints}
    if pairs.strip():
        params["pairs"] = pairs.strip()
    payload = state.get("/api/analyzer/problems", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _items(payload, "problems", "items")
    state.out.records(
        rows[: max(1, limit)],
        [
            ("SEVERITY", lambda r: str(r.get("severity") or r.get("level") or "-")),
            ("PROVIDER", lambda r: str(r.get("provider") or "-")),
            ("FEATURE", lambda r: str(r.get("feature") or "-")),
            ("TITLE", lambda r: _item_title(r)[:56]),
            ("TYPE", lambda r: str(r.get("item_type") or r.get("type_name") or r.get("type") or "-")),
            ("DETAIL", lambda r: _problem_detail(r)[:72]),
        ],
        title=f"Problems ({len(rows)})",
        empty="Nothing flagged.",
    )


@analyzer_app.command("state")
def analyzer_state(
    ctx: typer.Context,
    pairs: str = typer.Option("", "--pairs", "-p", help="Limit to these pair ids, comma separated."),
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows."),
    offset: int = typer.Option(0, "--offset", help="Skip this many rows."),
) -> None:
    """Show the analyzer view of stored state."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if pairs.strip():
        params["pairs"] = pairs.strip()
    payload = state.get("/api/analyzer/state", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _items(payload, "items", "rows", "state")
    state.out.records(
        rows,
        [
            ("KEY", lambda r: str(r.get("key") or r.get("item_key") or "-")[:34]),
            ("TITLE", lambda r: str(r.get("title") or "-")[:40]),
            ("TYPE", lambda r: str(r.get("type") or "-")),
            ("PROVIDER", lambda r: str(r.get("provider") or "-")),
            ("FEATURE", lambda r: str(r.get("feature") or "-")),
        ],
        title="Analyzer state",
        empty="No state rows.",
    )


@analyzer_app.command("attention")
def analyzer_attention(
    ctx: typer.Context,
    pairs: str = typer.Option("", "--pairs", "-p", help="Limit to these pair ids, comma separated."),
) -> None:
    """Show the system view: current mismatches, pending retries and blocked items."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get("/api/analyzer/system", params=_scope(pairs)))
    if state.out.json_mode:
        state.out.data(payload)
        return
    attention = as_dict(payload.get("attention"))
    counts = attention or payload
    state.out.kv(
        [
            ("Current mismatches", str(counts.get("current") or counts.get("mismatches") or 0)),
            ("Pending retries", str(counts.get("pending") or counts.get("retries") or 0)),
            ("Blocked", str(counts.get("blocked") or 0)),
            ("Flagged total", str(counts.get("total") or counts.get("flagged") or 0)),
        ],
        title="Needs attention",
    )
    rows = _items(attention or payload, "items", "rows")
    if rows:
        state.out.print()
        state.out.records(
            rows[:50],
            [
                ("TITLE", lambda r: str(r.get("title") or r.get("key") or "-")[:44]),
                ("PROVIDER", lambda r: str(r.get("provider") or "-")),
                ("FEATURE", lambda r: str(r.get("feature") or "-")),
                ("WHY", lambda r: str(r.get("reason") or r.get("state") or "-")[:36]),
            ],
            title="Flagged items",
        )


@analyzer_app.command("ratings")
def analyzer_ratings(
    ctx: typer.Context,
    pairs: str = typer.Option("", "--pairs", "-p", help="Limit to these pair ids, comma separated."),
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows."),
) -> None:
    """Audit ratings for disagreement between providers."""
    state: Ctx = ctx.obj
    payload = state.get("/api/analyzer/ratings-audit", params=_scope(pairs))
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _items(payload, "items", "rows", "audit")
    state.out.records(
        rows[: max(1, limit)],
        [
            ("TITLE", lambda r: str(r.get("title") or r.get("key") or "-")[:44]),
            ("TYPE", lambda r: str(r.get("type") or "-")),
            ("RATINGS", lambda r: str(r.get("ratings") or r.get("values") or "-")[:40]),
            ("WHY", lambda r: str(r.get("reason") or r.get("status") or "-")[:30]),
        ],
        title="Ratings audit",
        empty="Ratings agree everywhere.",
    )


@analyzer_app.command("tracker")
def analyzer_tracker(
    ctx: typer.Context,
    pairs: str = typer.Option("", "--pairs", "-p", help="Limit to these pair ids, comma separated."),
) -> None:
    """Show the CrossWatch tracker state the analyzer sees."""
    state: Ctx = ctx.obj
    payload = state.get("/api/analyzer/cw-state", params=_scope(pairs))
    if state.out.json_mode:
        state.out.data(payload)
        return
    import json as _json

    from rich.syntax import Syntax

    state.out.console.print(
        Syntax(_json.dumps(payload, indent=2, default=str), "json", theme="ansi_dark", background_color="default")
    )


@analyzer_app.command("activity")
def analyzer_activity(ctx: typer.Context) -> None:
    """Show per pair analyzer activity."""
    state: Ctx = ctx.obj
    payload = state.get("/api/analyzer/pair-activity")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _items(payload, "pairs", "items")
    state.out.records(
        rows,
        [
            ("PAIR", lambda r: str(r.get("pair") or r.get("id") or "-")),
            ("LAST RUN", lambda r: str(r.get("last_run") or r.get("updated_at") or "-")),
            ("FLAGGED", lambda r: str(r.get("flagged") or r.get("count") or 0)),
        ],
        title="Pair activity",
        empty="No analyzer activity yet.",
    )


@analyzer_app.command("detail")
def analyzer_detail(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    feature: str = typer.Argument(..., help="Feature, for example watchlist."),
    key: str = typer.Argument(..., help="Item key."),
    pairs: str = typer.Option("", "--pairs", "-p", help="Limit to these pair ids, comma separated."),
) -> None:
    """Show everything the analyzer knows about one item."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {"provider": provider, "feature": feature, "key": key}
    if pairs.strip():
        params["pairs"] = pairs.strip()
    payload = state.get("/api/analyzer/detail", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    import json as _json

    from rich.syntax import Syntax

    state.out.console.print(
        Syntax(_json.dumps(payload, indent=2, default=str), "json", theme="ansi_dark", background_color="default")
    )


@analyzer_app.command("fix")
def analyzer_fix(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    feature: str = typer.Argument(..., help="Feature, for example watchlist."),
    key: str = typer.Argument(..., help="Item key."),
    pairs: str = typer.Option("", "--pairs", "-p", help="Limit to these pair ids, comma separated."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Apply the suggested fix for one item."""
    state: Ctx = ctx.obj
    state.require_service("Applying an analyzer fix")
    if not yes and not typer.confirm(f"Apply the analyzer fix to {key}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    body = {"provider": provider, "feature": feature, "key": key}
    result = as_dict(state.post("/api/analyzer/fix", params=_scope(pairs), json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Fix rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Fix applied to {key}")


@analyzer_app.command("suggest")
def analyzer_suggest(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    feature: str = typer.Argument(..., help="Feature, for example watchlist."),
    key: str = typer.Argument(..., help="Item key."),
    pairs: str = typer.Option("", "--pairs", "-p", help="Limit to these pair ids, comma separated."),
) -> None:
    """Ask what the analyzer would do, without doing it."""
    state: Ctx = ctx.obj
    body = {"provider": provider, "feature": feature, "key": key}
    payload = state.post("/api/analyzer/suggest", params=_scope(pairs), json_body=body)
    if state.out.json_mode:
        state.out.data(payload)
        return
    import json as _json

    from rich.syntax import Syntax

    state.out.console.print(
        Syntax(_json.dumps(payload, indent=2, default=str), "json", theme="ansi_dark", background_color="default")
    )


@analyzer_app.command("drop")
def analyzer_drop(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    feature: str = typer.Argument(..., help="Feature, for example watchlist."),
    key: str = typer.Argument(..., help="Item key."),
    pairs: str = typer.Option("", "--pairs", "-p", help="Limit to these pair ids, comma separated."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Remove an item from the analyzer state."""
    state: Ctx = ctx.obj
    state.require_service("Dropping an analyzer item")
    if not yes and not typer.confirm(f"Drop {key} from the analyzer state?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    body = {"provider": provider, "feature": feature, "key": key}
    result = as_dict(state.delete("/api/analyzer/item", params=_scope(pairs), json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Drop rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Dropped {key}")


def register(app: typer.Typer) -> None:
    app.add_typer(analyzer_app, name="analyzer")
