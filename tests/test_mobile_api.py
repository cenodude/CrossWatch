from __future__ import annotations

import json

import pytest
from fastapi import HTTPException
from starlette.requests import Request


def _request(
    path: str,
    *,
    method: str = "POST",
    headers: dict[str, str] | None = None,
    client: tuple[str, int] = ("127.0.0.1", 12345),
) -> Request:
    raw_headers = [(b"host", b"testserver")]
    for k, v in (headers or {}).items():
        raw_headers.append((str(k).lower().encode("latin-1"), str(v).encode("latin-1")))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("latin-1"),
        "query_string": b"",
        "headers": raw_headers,
        "client": client,
        "server": ("testserver", 80),
    }
    return Request(scope)


def _json_body(resp) -> dict:
    return json.loads(resp.body.decode("utf-8"))


def test_mobile_pairing_claim_issues_scoped_token(monkeypatch) -> None:
    from api import mobileAPI as mobile

    mobile._PAIRING_CLAIM_FAILS.clear()
    cfg: dict = {"mobile_auth": {"enabled": True, "devices": [], "pairings": []}}
    monkeypatch.setattr(mobile, "load_config", lambda: cfg)
    monkeypatch.setattr(mobile, "save_config", lambda next_cfg: cfg.update(next_cfg))
    monkeypatch.setattr(mobile, "_activity_payload", lambda: ([], 0))
    monkeypatch.setattr(mobile, "_scheduler_label", lambda: ("Disabled", "Not scheduled"))
    monkeypatch.setattr(mobile, "_watching_label", lambda: "Nothing playing")
    monkeypatch.setattr(mobile, "_library_payload", lambda: [])

    start = mobile.mobile_pairing_start(
        _request("/api/mobile/pairing/start"),
        {"device_name": "Tablet", "scopes": ["read"]},
    )
    start_data = _json_body(start)
    code = start_data["code"]

    qr = mobile.mobile_pairing_qr(
        _request(f"/api/mobile/pairing/{start_data['id']}/qr.svg", method="GET"),
        start_data["id"],
    )
    assert qr.media_type == "image/svg+xml"
    assert qr.body.startswith(b"<?xml")

    claim = mobile.mobile_pairing_claim(
        _request("/api/mobile/pairing/claim"),
        {"code": code, "device_name": "Tablet"},
    )
    data = _json_body(claim)
    token = data["token"]
    assert data["device"]["name"] == "Tablet"
    assert data["scopes"] == ["read"]
    assert cfg["mobile_auth"]["devices"][0]["token_hash"]
    assert "token" not in cfg["mobile_auth"]["devices"][0]

    with pytest.raises(HTTPException) as missing:
        mobile.mobile_summary(_request("/api/mobile/summary", method="GET"))
    assert missing.value.status_code == 401

    authed = _request(
        "/api/mobile/summary",
        method="GET",
        headers={"authorization": f"Bearer {token}"},
    )
    summary = mobile.mobile_summary(authed)
    assert _json_body(summary)["server_name"] == "CrossWatch"

    with pytest.raises(HTTPException) as forbidden:
        mobile.mobile_run_sync(authed)
    assert forbidden.value.status_code == 403
    assert "actions" in str(forbidden.value.detail)


def test_mobile_pairing_qr_requires_qrcode(monkeypatch) -> None:
    from api import mobileAPI as mobile

    cfg: dict = {"mobile_auth": {"enabled": True, "devices": [], "pairings": []}}
    monkeypatch.setattr(mobile, "load_config", lambda: cfg)
    monkeypatch.setattr(mobile, "save_config", lambda next_cfg: cfg.update(next_cfg))

    start = mobile.mobile_pairing_start(
        _request("/api/mobile/pairing/start"),
        {"device_name": "Tablet", "scopes": ["read"]},
    )
    start_data = _json_body(start)

    real_import = mobile.importlib.import_module

    def fake_import(name: str):
        if name.startswith("qrcode"):
            raise ImportError(name)
        return real_import(name)

    monkeypatch.setattr(mobile.importlib, "import_module", fake_import)
    with pytest.raises(HTTPException) as missing:
        mobile.mobile_pairing_qr(
            _request(f"/api/mobile/pairing/{start_data['id']}/qr.svg", method="GET"),
            start_data["id"],
        )
    assert missing.value.status_code == 503
    assert missing.value.detail == "mobile_qr_dependency_missing"


def test_mobile_pairing_claim_rate_limits_bad_codes(monkeypatch) -> None:
    from api import mobileAPI as mobile

    mobile._PAIRING_CLAIM_FAILS.clear()
    cfg: dict = {"mobile_auth": {"enabled": True, "devices": [], "pairings": []}}
    monkeypatch.setattr(mobile, "load_config", lambda: cfg)

    req = _request("/api/mobile/pairing/claim", client=("203.0.113.10", 54321))
    for _ in range(mobile.PAIRING_CLAIM_FAIL_LIMIT):
        with pytest.raises(HTTPException) as invalid:
            mobile.mobile_pairing_claim(req, {"code": "NOPE123456", "device_name": "Phone"})
        assert invalid.value.status_code == 404

    with pytest.raises(HTTPException) as limited:
        mobile.mobile_pairing_claim(req, {"code": "NOPE123456", "device_name": "Phone"})
    assert limited.value.status_code == 429
    assert limited.value.detail == "pairing_claim_rate_limited"
