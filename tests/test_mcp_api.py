# tests/test_mcp_api.py
# CrossWatch - Read-only MCP endpoint tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.responses import JSONResponse

from services.log_archive import LogArchive

MONITOR_ID = "c" * 32
PLAIN_ID = "d" * 32
PROFILE_ID = "cccccccccccc4ccc8ccccccccccccccc"
PATH = "/api/mcp"


@pytest.fixture()
def store(tmp_path: Path):
    result = LogArchive(tmp_path / "logs.db")
    yield result
    result.close()


@pytest.fixture()
def gated(config_base: Path, monkeypatch: pytest.MonkeyPatch, store: LogArchive):
    import importlib

    from cw_platform import config_base as cfg_base

    importlib.reload(cfg_base)
    from api import apiTokensAPI

    importlib.reload(apiTokensAPI)

    import crosswatch
    from api import appAuthAPI as auth
    from api import logsAPI
    from cw_platform.config_base import load_config

    def _user(name: str, permissions: dict[str, bool]) -> dict[str, Any]:
        return {
            "username": name,
            "enabled": True,
            "role": "user",
            "profile_id": PROFILE_ID,
            "permissions": permissions,
            "password": auth._password_hash("secrett3"),
        }

    cfg = {
        "pairs": [{"id": "p1", "name": "Main", "source": "PLEX", "target": "TRAKT", "mode": "one-way", "features": {}}],
        "trakt": {"client_id": "abcdefabcdefabcdefabcdef", "access_token": "tok-should-never-leak"},
        "user_profiles": {PROFILE_ID: {"label": "Ops", "instances": {}}},
        "app_auth": {
            "enabled": True,
            "username": "admin",
            "password": auth._password_hash("secrett1"),
            "sessions": [],
            "api_tokens": [],
            "users": {
                MONITOR_ID: _user("monitor", {"monitor": True}),
                PLAIN_ID: _user("plain", {"dashboard": True, "watchlist": True, "playback": True, "write": True}),
            },
        },
    }
    (config_base / "config.json").write_text(json.dumps(cfg), encoding="utf-8")
    apiTokensAPI._TOUCH_CACHE.clear()
    monkeypatch.setattr(crosswatch, "load_config", load_config)
    monkeypatch.setattr(crosswatch, "app_resolve_api_token", apiTokensAPI.resolve_api_token)
    monkeypatch.setattr(crosswatch, "app_touch_api_token", lambda _tid: None)
    monkeypatch.setattr(logsAPI, "archive", lambda: store)
    return apiTokensAPI


def _tokens(mod: Any) -> dict[str, str]:
    from cw_platform.config_base import load_config

    return {
        "admin": mod.issue_api_token(load_config(), name="admin")[0],
        "monitor": mod.issue_api_token(load_config(), name="ai", user_id="monitor")[0],
        "plain": mod.issue_api_token(load_config(), name="plain", user_id="plain")[0],
    }


def _rpc(client: Any, token: str, method: str, params: dict[str, Any] | None = None, *, msg_id: Any = 1) -> Any:
    body: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if msg_id is not None:
        body["id"] = msg_id
    if params is not None:
        body["params"] = params
    return client.post(PATH, json=body, headers={"Authorization": f"Bearer {token}"})


def _call(client: Any, token: str, name: str, **arguments: Any) -> dict[str, Any]:
    res = _rpc(client, token, "tools/call", {"name": name, "arguments": arguments})
    assert res.status_code == 200
    return res.json()["result"]


def test_only_admin_and_monitor_tokens_reach_the_endpoint(gated) -> None:
    from fastapi.testclient import TestClient

    import crosswatch
    from cw_platform.access_policy import monitor_api_allowed

    tokens = _tokens(gated)
    client = TestClient(crosswatch.app)
    ping = {"jsonrpc": "2.0", "id": 1, "method": "ping"}

    assert client.post(PATH, json=ping).status_code == 401
    assert client.post(PATH, json=ping, headers={"Authorization": "Bearer cwt_nope"}).status_code == 401
    assert _rpc(client, tokens["plain"], "ping").status_code == 403
    for name in ("admin", "monitor"):
        res = _rpc(client, tokens[name], "ping")
        assert res.status_code == 200
        assert res.json() == {"jsonrpc": "2.0", "id": 1, "result": {}}
        assert res.headers["cache-control"] == "no-store"
        assert client.get(PATH, headers={"Authorization": f"Bearer {tokens[name]}"}).status_code == 405

    monitor = {"is_admin": False, "permissions": {"monitor": True}}
    assert monitor_api_allowed(monitor, PATH, "POST") is True
    assert monitor_api_allowed(monitor, "/api/run", "POST") is False
    assert monitor_api_allowed(monitor, PATH, "DELETE") is False
    assert client.post("/api/run", json={}, headers={"Authorization": f"Bearer {tokens['monitor']}"}).status_code == 403


def test_read_only_token_is_a_monitor_token_without_a_managed_user(gated, monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    import crosswatch
    from api import healthAPI
    from cw_platform.config_base import load_config

    tokens = _tokens(gated)
    client = TestClient(crosswatch.app)
    admin = {"Authorization": f"Bearer {tokens['admin']}"}
    url = "/api/app-auth/tokens"

    made = client.post(url, json={"name": "AI Cursor", "access": "monitor", "expires_days": 3}, headers=admin)
    assert made.status_code == 200
    entry = made.json()["entry"]
    assert entry["access"] == "monitor"
    assert entry["user_id"] == "administrator"
    ai = {"Authorization": f"Bearer {made.json()['token']}"}

    who = client.get(f"{url}/whoami", headers=ai).json()
    assert who["user"]["is_admin"] is False
    assert who["token_id"] == entry["id"]
    assert client.post(PATH, json={"jsonrpc": "2.0", "id": 1, "method": "ping"}, headers=ai).status_code == 200
    assert client.get("/api/pairs", headers=ai).status_code == 200
    for method, path in (("GET", "/api/config"), ("POST", "/api/run"), ("POST", url), ("DELETE", f"{url}/{entry['id']}"), ("GET", "/api/logs/archive")):
        assert client.request(method, path, headers=ai, json={} if method == "POST" else None).status_code == 403, path
    assert client.get("/api/app-auth/users", headers=ai).status_code in (401, 403)
    assert [row["id"] for row in client.get(url, headers=ai).json()["tokens"]] == [entry["id"]]

    listed = {row["name"]: row["access"] for row in client.get(url, headers=admin).json()["tokens"]}
    assert listed == {"admin": "", "ai": "", "plain": "", "AI Cursor": "monitor"}

    seen: list[list[str]] = []

    def details(request: Any, strict: int = 0) -> Any:
        seen.append([row["name"] for row in healthAPI._visible_tokens(load_config(), request)])
        return JSONResponse({"ok": True})

    monkeypatch.setattr(healthAPI, "api_health_details", details)
    _call(client, made.json()["token"], "get_health")
    assert seen == [["AI Cursor"]]

    plain = {"Authorization": f"Bearer {tokens['plain']}"}
    assert client.post(url, json={"name": "x", "access": "monitor"}, headers=plain).status_code == 403
    assert client.post(url, json={"name": "x", "access": "monitor", "user_id": "plain"}, headers=admin).status_code == 400
    assert client.post(url, json={"name": "x", "access": "root"}, headers=admin).status_code == 400

    assert client.delete(f"{url}/{entry['id']}", headers=admin).status_code == 200
    assert client.post(PATH, json={"jsonrpc": "2.0", "id": 1, "method": "ping"}, headers=ai).status_code == 401


def test_endpoint_stays_closed_without_app_auth(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    from cw_platform import config_base as cfg_base

    importlib.reload(cfg_base)
    from fastapi.testclient import TestClient

    import crosswatch
    from cw_platform.config_base import load_config

    (config_base / "config.json").write_text(json.dumps({"app_auth": {"enabled": False}}), encoding="utf-8")
    monkeypatch.setattr(crosswatch, "load_config", load_config)
    monkeypatch.setattr(crosswatch, "app_auth_setup_lock_required", lambda _cfg: False)
    monkeypatch.setattr(crosswatch, "app_auth_required", lambda _cfg: False)

    res = TestClient(crosswatch.app).post(PATH, json={"jsonrpc": "2.0", "id": 1, "method": "ping"})

    assert res.status_code == 403


def test_protocol_handshake_and_errors(gated) -> None:
    from fastapi.testclient import TestClient

    import crosswatch
    from api import mcpAPI

    token = _tokens(gated)["monitor"]
    client = TestClient(crosswatch.app)

    init = _rpc(client, token, "initialize", {"protocolVersion": "2025-06-18", "capabilities": {}}).json()["result"]
    assert init["protocolVersion"] == "2025-06-18"
    assert init["capabilities"] == {"tools": {"listChanged": False}}
    assert init["serverInfo"]["name"] == "crosswatch"
    assert "wiki.crosswatch.app" in init["instructions"]
    unknown = _rpc(client, token, "initialize", {"protocolVersion": "1999-01-01"}).json()["result"]
    assert unknown["protocolVersion"] == mcpAPI.PROTOCOL_VERSIONS[0]

    note = _rpc(client, token, "notifications/initialized", msg_id=None)
    assert note.status_code == 202
    assert note.content == b""

    tools = _rpc(client, token, "tools/list").json()["result"]["tools"]
    assert [tool["name"] for tool in tools] == list(mcpAPI.TOOLS)
    assert all(tool["annotations"]["readOnlyHint"] is True and tool["inputSchema"]["type"] == "object" for tool in tools)

    assert _rpc(client, token, "resources/list", msg_id="a").json() == {
        "jsonrpc": "2.0",
        "id": "a",
        "error": {"code": -32601, "message": "Method not found: resources/list"},
    }
    assert _rpc(client, token, "tools/call", {"name": "nope"}).json()["error"]["code"] == -32602
    headers = {"Authorization": f"Bearer {token}"}
    broken = client.post(PATH, content=b"{not json", headers=headers)
    assert broken.status_code == 400
    assert broken.json()["error"]["code"] == -32700
    assert client.post(PATH, json={"id": 1, "method": "ping"}, headers=headers).json()["error"]["code"] == -32600
    assert client.post(PATH, content=b" " * (mcpAPI.MAX_BODY_BYTES + 1), headers=headers).status_code == 413

    batch = client.post(
        PATH,
        json=[{"jsonrpc": "2.0", "id": 1, "method": "ping"}, {"jsonrpc": "2.0", "method": "notifications/initialized"}],
        headers=headers,
    )
    assert batch.json() == [{"jsonrpc": "2.0", "id": 1, "result": {}}]

    ping = {"jsonrpc": "2.0", "id": 1, "method": "ping"}
    for version in mcpAPI.PROTOCOL_VERSIONS:
        assert client.post(PATH, json=ping, headers={**headers, "MCP-Protocol-Version": version}).status_code == 200
    stale = client.post(PATH, json=ping, headers={**headers, "MCP-Protocol-Version": "2024-11-05"})
    assert stale.status_code == 400
    assert "Unsupported protocol version" in stale.json()["error"]["message"]
    assert client.post(PATH, json=[ping], headers={**headers, "MCP-Protocol-Version": "2025-06-18"}).status_code == 400
    assert client.post(PATH, json=[ping] * mcpAPI.MAX_BATCH, headers=headers).status_code == 200
    assert client.post(PATH, json=[ping] * (mcpAPI.MAX_BATCH + 1), headers=headers).status_code == 400
    assert client.post(PATH, json=[], headers=headers).status_code == 400

    assert client.post(PATH, json=ping, headers={**headers, "Origin": "http://testserver"}).status_code == 200
    foreign = client.post(PATH, json=ping, headers={**headers, "Origin": "http://evil.test"})
    assert foreign.status_code == 403
    assert foreign.json()["error"] == "Origin mismatch"

    def chunks() -> Any:
        for _ in range(3):
            yield b" " * mcpAPI.MAX_BODY_BYTES

    assert client.post(PATH, content=chunks(), headers=headers).status_code == 413


def test_log_tools_read_tail_filter_and_context(gated, store: LogArchive) -> None:
    from fastapi.testclient import TestClient

    import crosswatch

    token = _tokens(gated)["monitor"]
    client = TestClient(crosswatch.app)
    sid = store.start("run-one", [{"id": "p1", "source": "PLEX", "target": "TRAKT"}])
    for index in range(30):
        level = "ERROR" if index == 12 else "INFO"
        store.append("sync", "SYNC", f"[TRAKT] {level} item {index} token=supersecretvalue")
    store.finish(sid, "failed")
    store.append("watcher", "PLEX", "INFO playing")

    sessions = json.loads(_call(client, token, "list_log_sessions")["content"][0]["text"])
    assert [row["id"] for row in sessions] == [sid]
    assert sessions[0]["errors"] == 1
    assert sessions[0]["lines"] == 30
    assert sessions[0]["status"] == "failed"
    watcher = json.loads(_call(client, token, "list_log_sessions", channel="watcher")["content"][0]["text"])
    assert len(watcher) == 1

    tail = _call(client, token, "read_logs", session_id=sid, limit=5)["content"][0]["text"].splitlines()
    assert tail[0].endswith("lines 26-30 of 30 matching")
    assert [line.split(" item ")[1].split()[0] for line in tail[1:]] == ["25", "26", "27", "28", "29"]
    assert "supersecretvalue" not in "\n".join(tail)

    head = _call(client, token, "read_logs", session_id=sid, limit=2, offset=0)["content"][0]["text"].splitlines()
    assert head[0].endswith("lines 1-2 of 30 matching")

    errors = _call(client, token, "read_logs", session_id=sid, level="error", provider="trakt")["content"][0]["text"].splitlines()
    assert errors[0].endswith("lines 1-1 of 1 matching")
    assert " [TRAKT] ERROR " in errors[1]
    line_id = int(errors[1].split()[0])

    context = _call(client, token, "read_log_context", session_id=sid, line_id=line_id)["content"][0]["text"].splitlines()
    assert len(context) == 11
    assert any(line.startswith(f"{line_id} ") for line in context)

    with store.lock:
        store.db.execute(
            "INSERT INTO lines(session_id,ts,level,provider,text,pair_id,run_id) VALUES(?,?,?,?,?,'','')",
            (sid, 1.0, "ERROR", "PLEX", "GET https://plex.test/x?access_token=oldleak1 and /webhook/plex?profile=oldleak2 refresh_token: oldleak3"),
        )
        store.db.commit()
    old = _call(client, token, "read_logs", session_id=sid, query="plex.test")["content"][0]["text"]
    assert "plex.test" in old
    assert "oldleak" not in old
    assert "oldleak" not in _call(client, token, "read_log_context", session_id=sid, line_id=int(old.splitlines()[1].split()[0]))["content"][0]["text"]

    missing = _call(client, token, "read_logs", session_id="nope")
    assert missing["isError"] is True
    bad_level = _call(client, token, "read_logs", session_id=sid, level="LOUD")
    assert bad_level["isError"] is True
    assert "level must be one of" in bad_level["content"][0]["text"]


class _BridgeTransport:
    def __init__(self, client: Any, token: str) -> None:
        self.client = client
        self.token = token

    def request(self, method: str, path: str, *, params: Any = None, json_body: Any = None) -> Any:
        from cli._errors import ApiError

        res = self.client.request(method, path, json=json_body, headers={"X-CW-Token": self.token})
        payload = res.json() if "application/json" in res.headers.get("content-type", "") else res.text
        if res.status_code >= 400:
            raise ApiError(res.status_code, payload, method=method, path=path)
        return payload


def _bridge(client: Any, token: str, output: str = "json") -> Any:
    from cli._context import Ctx
    from cli._render import Output

    state = Ctx(url="http://cw.test", out=Output(output))
    state._http = _BridgeTransport(client, token)  # type: ignore[assignment]
    return state


def test_cli_bridge_relays_stdio_messages(gated) -> None:
    import io

    from fastapi.testclient import TestClient

    import crosswatch
    from cli._errors import TransportUnavailable
    from cli.commands import mcp as cli_mcp

    tokens = _tokens(gated)
    client = TestClient(crosswatch.app)
    lines = [
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}}),
        json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        "",
        json.dumps({"jsonrpc": "2.0", "id": "t", "method": "tools/list"}),
        "{broken",
    ]
    stdout = io.BytesIO()
    cli_mcp.serve(_bridge(client, tokens["monitor"]), io.BytesIO(("\n".join(lines) + "\n").encode("utf-8")), stdout)
    replies = [json.loads(line) for line in stdout.getvalue().decode("utf-8").splitlines()]

    assert [reply.get("id") for reply in replies] == [1, "t", None]
    assert replies[0]["result"]["serverInfo"]["name"] == "crosswatch"
    assert len(replies[1]["result"]["tools"]) == 13
    assert replies[2]["error"]["code"] == -32700

    denied = cli_mcp.relay(_bridge(client, tokens["plain"]), json.dumps({"jsonrpc": "2.0", "id": 7, "method": "ping"}))
    assert denied["id"] == 7
    assert denied["error"]["code"] == cli_mcp.BRIDGE_ERROR
    assert cli_mcp.relay(_bridge(client, tokens["plain"]), json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"})) is None

    class Down:
        def request(self, *_args: Any, **_kwargs: Any) -> Any:
            raise TransportUnavailable("Cannot reach CrossWatch at http://cw.test")

    offline = _bridge(client, "")
    offline._http = Down()  # type: ignore[assignment]
    batch = cli_mcp.relay(offline, json.dumps([{"jsonrpc": "2.0", "id": 1, "method": "ping"}, {"jsonrpc": "2.0", "method": "x"}]))
    assert batch == [{"jsonrpc": "2.0", "id": 1, "error": {"code": cli_mcp.BRIDGE_ERROR, "message": "Cannot reach CrossWatch at http://cw.test"}}]


def test_cli_check_lists_tools_and_reports_a_refused_token(gated, capsys: pytest.CaptureFixture[str]) -> None:
    from types import SimpleNamespace

    from fastapi.testclient import TestClient

    import crosswatch
    from cli._app import describe_group
    from cli._errors import ApiError
    from cli.commands import mcp as cli_mcp

    tokens = _tokens(gated)
    client = TestClient(crosswatch.app)

    cli_mcp.mcp_check(SimpleNamespace(obj=_bridge(client, tokens["monitor"])))
    payload = json.loads(capsys.readouterr().out)
    assert payload["endpoint"] == "http://cw.test/api/mcp"
    assert payload["server"]["name"] == "crosswatch"
    assert "read_logs" in payload["tools"]

    with pytest.raises(ApiError) as refused:
        cli_mcp.mcp_check(SimpleNamespace(obj=_bridge(client, tokens["plain"])))
    assert refused.value.status == 403
    assert [name for name, _help in describe_group("mcp")] == ["check", "serve"]


def test_settings_card_ships_with_its_script_for_admins_only() -> None:
    import ui_frontend

    admin = ui_frontend.get_index_html(include_admin=True)
    user = ui_frontend.get_index_html(include_admin=False)
    script = Path("assets/js/mcp-access.js").read_text("utf-8")

    assert 'id="mcp_access"' in admin
    assert "/assets/js/mcp-access.js" in admin
    assert 'id="mcp_access"' not in user
    assert "/assets/js/mcp-access.js" not in user
    for value in ("claude-code", "claude-desktop", "codex", "cursor", "gemini-cli", "lm-studio", "vscode", "other"):
        assert f'data-mcp-client="{value}"' in admin
    for value in ("claude-code", "claude-desktop", "claude-desktop-docker", "codex", "cursor", "gemini-cli", "lm-studio", "vscode", "other"):
        assert f'"{value}": {{' in script or f"{value}: {{" in script
    assert 'data-mcp-where="local"' in admin
    assert "/api/mcp" in script


def test_state_tools_return_redacted_read_only_data(gated, monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    import crosswatch
    from api import healthAPI, versionAPI
    from cw_platform.config_base import load_config

    token = _tokens(gated)["monitor"]
    client = TestClient(crosswatch.app)
    before = json.dumps(load_config().get("pairs"), sort_keys=True)

    full = _call(client, token, "get_config")
    assert full["isError"] is False
    assert "tok-should-never-leak" not in full["content"][0]["text"]
    assert "abcdefabcdefabcdefabcdef" not in full["content"][0]["text"]
    assert "cwt_" not in full["content"][0]["text"]
    assert "app_auth" not in json.loads(full["content"][0]["text"])
    assert _call(client, token, "get_config", section="app_auth")["isError"] is True
    section = json.loads(_call(client, token, "get_config", section="trakt")["content"][0]["text"])
    assert set(section) == {"trakt"}
    assert _call(client, token, "get_config", section="nope")["isError"] is True

    pairs = json.loads(_call(client, token, "list_pairs")["content"][0]["text"])
    assert [(pair["id"], pair["source"], pair["target"]) for pair in pairs] == [("p1", "PLEX", "TRAKT")]
    assert json.dumps(load_config().get("pairs"), sort_keys=True) == before

    monkeypatch.setattr(healthAPI, "api_health_details", lambda request, strict=0: JSONResponse({"ok": True, "status": "ok", "issues": []}))
    monkeypatch.setattr(versionAPI, "get_version", lambda: {"current": "1.2.3"})
    health = json.loads(_call(client, token, "get_health")["content"][0]["text"])
    assert health == {"ok": True, "status": "ok", "issues": [], "version": {"current": "1.2.3"}}

    for name, arguments in (
        ("get_last_sync", {}),
        ("get_watcher_status", {}),
        ("search_events", {"query": "matrix", "category": "problems", "domain": "scrobble", "feature": "history", "limit": 5}),
        ("get_item_events", {"item_key": "tmdb:603#movie", "limit": 5}),
        ("get_anime_mapping_status", {}),
        ("list_captures", {"limit": 5}),
    ):
        result = _call(client, token, name, **arguments)
        assert result["isError"] is False, result["content"][0]["text"]
        assert isinstance(json.loads(result["content"][0]["text"]), dict)

    assert _call(client, token, "get_item_events")["isError"] is True

    from api import scrobbleAPI
    from services import analyzer

    stream = {"title": "Heat", "state": "playing", "account": "sam", "cover": "http://plex.test/thumb?X-Plex-Token=leak", "updated": 5}
    monkeypatch.setattr(scrobbleAPI, "_cw_load_state", lambda: {"v": 2, "streams": {"s1": stream}})
    watcher = json.loads(_call(client, token, "get_watcher_status")["content"][0]["text"])
    assert [(row["title"], row["state"]) for row in watcher["now_playing"]] == [("Heat", "playing")]
    assert "leak" not in json.dumps(watcher)

    seen: dict[str, Any] = {}

    def analysis(pairs: Any, **kwargs: Any) -> dict[str, Any]:
        seen.update(pairs=pairs, **kwargs)
        rows = [{"severity": "warn", "type": "missing_peer", "feature": "history" if index % 2 else "ratings", "key": f"k{index}"} for index in range(6)]
        return {"problems": rows, "summary": {"total": 6}, "attention": {"rows": rows, "counts": {"total": 6}}}

    monkeypatch.setattr(analyzer, "_analysis_with_saved_mappings", analysis)
    problems = json.loads(_call(client, token, "get_analyzer_problems", pair_id="p1", feature="History", limit=2, offset=1)["content"][0]["text"])
    assert seen == {"pairs": "p1", "include_system": False, "include_hints": False}
    assert problems["summary"] == {"total": 6}
    assert problems["attention"] == {"total": 6}
    assert problems["total"] == 3
    assert [row["key"] for row in problems["problems"]] == ["k3", "k5"]

    def boom(request: Any, strict: int = 0) -> Any:
        raise OSError("disk path /secret/place")

    monkeypatch.setattr(healthAPI, "api_health_details", boom)
    failed = _call(client, token, "get_health")
    assert failed["isError"] is True
    assert failed["content"][0]["text"] == "get_health failed: OSError"
