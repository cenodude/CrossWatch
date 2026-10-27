# CrossWatch test scripts
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from cli._context import Ctx
from cli._render import Output
from cli._errors import ApiError
from cli._transport import Transport
from cli.commands.auth import auth_login


class FakeTransport(Transport):
    name = "fake"

    def __init__(self, routes: dict[tuple[str, str], Any]) -> None:
        self.routes = routes
        self.calls: list[tuple[str, str, Any]] = []

    def request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: Any = None) -> Any:
        self.calls.append((method.upper(), path, json_body))
        value = self.routes.get((method.upper(), path))
        if value is None:
            raise ApiError(404, {"error": "not found"}, method=method, path=path)
        return value(params, json_body) if callable(value) else value


MANIFEST = [{"name": "SILO", "label": "Silo", "flow": "device_code", "fields": [
    {"key": "silo.server_url", "label": "Server URL", "type": "text", "required": True},
    {"key": "silo.verify_ssl", "label": "Verify SSL", "type": "bool"},
]}]


def _routes(profiles: list[dict[str, Any]]) -> tuple[dict[tuple[str, str], Any], dict[str, Any]]:
    state = {"connected": False}

    def select(params: Any, body: Any) -> dict[str, Any]:
        state["selected"] = body
        state["connected"] = True
        return {"ok": True, "profile_name": "Kid"}

    routes = {
        ("GET", "/api/auth/providers"): MANIFEST,
        ("POST", "/api/silo/server"): {"ok": True, "server_url": "http://silo:8090"},
        ("POST", "/api/silo/device/start"): {"ok": True, "user_code": "1234-5678", "verification_uri": "http://silo:8090/activate", "expires_at": 4_000_000_000, "interval": 3},
        ("POST", "/api/silo/device/poll"): {"ok": True, "status": "authorized", "profiles": profiles},
        ("GET", "/api/status"): lambda p, b: {"providers": {"SILO": {"connected": state["connected"]}}},
        ("GET", "/api/silo/profiles"): {"ok": True, "profiles": profiles},
        ("POST", "/api/silo/profile/select"): select,
    }
    return routes, state


def _run(routes: dict[tuple[str, str], Any], **kwargs: Any) -> FakeTransport:
    http = FakeTransport(routes)
    ctx = Ctx(url="http://cw.test", out=Output("json"))
    ctx._http = http  # type: ignore[assignment]
    auth_login(SimpleNamespace(obj=ctx), provider="silo", fields=["silo.server_url=http://silo:8090"], instance="default",
               interactive=False, wait=True, timeout=30.0, interval=0.0, **kwargs)
    return http


def test_cli_login_sends_server_then_device_flow_then_pin_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("cli.commands.auth.time.sleep", lambda _s: None)
    routes, state = _routes([{"id": "p-1", "name": "Alice", "has_pin": False}, {"id": "p-2", "name": "Kid", "has_pin": True}])

    http = _run(routes, profile="kid", pin="1234")

    assert ("POST", "/api/silo/server", {"server_url": "http://silo:8090"}) in http.calls
    assert [c[1] for c in http.calls].index("/api/silo/server") < [c[1] for c in http.calls].index("/api/silo/device/start")
    assert state["selected"] == {"profile_id": "p-2", "pin": "1234"}


def test_cli_login_needs_profile_choice_when_not_interactive(monkeypatch: pytest.MonkeyPatch) -> None:
    from cli._errors import CLIError

    monkeypatch.setattr("cli.commands.auth.time.sleep", lambda _s: None)
    routes, _state = _routes([{"id": "p-1", "name": "Alice"}, {"id": "p-2", "name": "Kid"}])

    with pytest.raises(CLIError, match="several profiles"):
        _run(routes, profile="", pin="")
    with pytest.raises(CLIError, match="no profile named"):
        _run(routes, profile="nobody", pin="")


def test_manifest_exposes_cli_fields() -> None:
    from providers.auth._auth_SILO import SiloAuth

    keys = [f["key"] for f in SiloAuth().manifest().fields]
    assert keys == ["silo.server_url", "silo.verify_ssl"]
