# /cli/commands/mcp.py
# CrossWatch - CLI MCP bridge and check for AI clients
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import sys
from typing import Any, BinaryIO

import typer

from .._context import Ctx
from .._errors import ApiError, CLIError
from .._util import as_dict

mcp_app = typer.Typer(help="Connect AI clients to CrossWatch over MCP.", no_args_is_help=True)

MCP_PATH = "/api/mcp"
PROTOCOL_VERSION = "2025-06-18"
BRIDGE_ERROR = -32000


def _failures(message: Any, text: str) -> Any:
    items = message if isinstance(message, list) else [message]
    replies = [
        {"jsonrpc": "2.0", "id": item.get("id"), "error": {"code": BRIDGE_ERROR, "message": text}}
        for item in items
        if isinstance(item, dict) and "id" in item and isinstance(item.get("method"), str)
    ]
    if not replies:
        return None
    return replies if isinstance(message, list) else replies[0]


def relay(state: Ctx, line: str) -> Any:
    try:
        message = json.loads(line)
    except ValueError:
        return {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}
    try:
        reply = state.http.request("POST", MCP_PATH, json_body=message)
    except ApiError as err:
        if isinstance(err.payload, dict) and err.payload.get("jsonrpc") == "2.0":
            return err.payload
        return _failures(message, err.message)
    except CLIError as err:
        return _failures(message, err.message)
    return reply if isinstance(reply, (dict, list)) else None


def serve(state: Ctx, stdin: BinaryIO, stdout: BinaryIO) -> None:
    for raw in iter(stdin.readline, b""):
        line = raw.decode("utf-8", errors="replace").strip()
        if not line:
            continue
        reply = relay(state, line)
        if reply is None:
            continue
        stdout.write(json.dumps(reply, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n")
        stdout.flush()


def _call(state: Ctx, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {"jsonrpc": "2.0", "id": 1, "method": method}
    if params is not None:
        body["params"] = params
    reply = as_dict(state.http.request("POST", MCP_PATH, json_body=body))
    error = as_dict(reply.get("error"))
    if error:
        raise CLIError(f"MCP {method} failed: {error.get('message') or 'unknown error'}")
    return as_dict(reply.get("result"))


@mcp_app.command("serve")
def mcp_serve(ctx: typer.Context) -> None:
    """Run a stdio MCP server that relays to CrossWatch, for clients that start a local command."""
    state: Ctx = ctx.obj
    state.require_service("MCP")
    try:
        serve(state, sys.stdin.buffer, sys.stdout.buffer)
    except KeyboardInterrupt:
        return


@mcp_app.command("check")
def mcp_check(ctx: typer.Context) -> None:
    """Check that the MCP endpoint answers with this token and list its tools."""
    state: Ctx = ctx.obj
    state.require_service("MCP")
    client = {"name": "crosswatch-cli", "version": "1"}
    init = _call(state, "initialize", {"protocolVersion": PROTOCOL_VERSION, "capabilities": {}, "clientInfo": client})
    tools = [as_dict(tool) for tool in (_call(state, "tools/list").get("tools") or []) if isinstance(tool, dict)]
    server = as_dict(init.get("serverInfo"))
    endpoint = state.url + MCP_PATH
    if state.out.json_mode:
        state.out.data(
            {
                "endpoint": endpoint,
                "server": server,
                "protocol": init.get("protocolVersion"),
                "tools": [str(tool.get("name") or "") for tool in tools],
            }
        )
        return
    state.out.kv(
        [
            ("Endpoint", endpoint),
            ("Server", f"{server.get('title') or server.get('name') or '-'} {server.get('version') or ''}".strip()),
            ("Protocol", str(init.get("protocolVersion") or "-")),
        ],
        title="MCP",
    )
    state.out.print()
    state.out.table(
        ["TOOL", "DESCRIPTION"],
        [[str(tool.get("name") or ""), str(tool.get("description") or "")] for tool in tools],
        title="Tools",
    )


def register(app: typer.Typer) -> None:
    app.add_typer(mcp_app, name="mcp")
