from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import pytest


@dataclass
class ResponseStub:
    status_code: int = 200
    headers: Mapping[str, str] | None = None
    payload: Mapping[str, Any] | None = None
    text: str = "{}"

    @property
    def ok(self) -> bool:
        return 200 <= int(self.status_code) < 400

    def json(self) -> Mapping[str, Any]:
        return dict(self.payload or {})


def test_anilist_health_ok(monkeypatch: pytest.MonkeyPatch):
    import sync._mod_ANILIST as m

    monkeypatch.setattr(m, "request_with_retries", lambda *_a, **_kw: ResponseStub(200, {}, {}))
    mod = m.ANILISTModule({"anilist": {"access_token": "tok"}})
    h = mod.health()
    assert h.get("ok") is True
    assert h.get("status") in ("ok", "degraded")


def test_simkl_health_auth_failed(monkeypatch: pytest.MonkeyPatch):
    import sync._mod_SIMKL as m

    monkeypatch.setattr(m, "request_with_retries", lambda *_a, **_kw: ResponseStub(401, {}, {}))
    mod = m.SIMKLModule({"simkl": {"api_key": "k", "access_token": "tok"}})
    h = mod.health()
    assert h.get("ok") is False
    assert h.get("status") == "auth_failed"
