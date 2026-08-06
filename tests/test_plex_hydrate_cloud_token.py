from __future__ import annotations

from typing import Any

import pytest

PMS = "http://pms.local:32400"


class _Resp:
    def __init__(self, status: int, ids: list[str] | None = None) -> None:
        self.status_code = status
        self.ok = 200 <= status < 300
        self.headers = {"content-type": "application/json"}
        self._ids = ids or []

    def json(self) -> dict[str, Any]:
        return {"MediaContainer": {"Metadata": [{"Guid": [{"id": i} for i in self._ids]}]}}

    @property
    def text(self) -> str:
        return "{}"


@pytest.fixture
def plex(monkeypatch):
    from providers.sync.plex import _common as c

    with c._HYDRATE_LOCK:
        c._GUID_CACHE.clear()
        c._HYDRATE_404.clear()
    calls: list[tuple[str, str]] = []

    def _get(url, headers=None, params=None, timeout=None):
        tok = (headers or {}).get("X-Plex-Token", "")
        calls.append((url, tok))
        if url.startswith(c.METADATA):
            return _Resp(200, ["tmdb://603"]) if tok == "ACCOUNT" else _Resp(401)
        return _Resp(404)

    monkeypatch.setattr(c.requests, "get", _get)
    return c, calls


def test_cloud_url_uses_the_account_token(plex) -> None:
    c, calls = plex
    c.configure_plex_context(baseurl=PMS, token="PMS-TOKEN", account_token="ACCOUNT")

    ids = c.hydrate_external_ids("PMS-TOKEN", "1234")

    assert ids == {"tmdb": "603"}
    pms_call = next(u for u, _ in calls if u.startswith(PMS))
    meta_tok = next(t for u, t in calls if u.startswith(c.METADATA))
    assert pms_call.endswith("/library/metadata/1234")
    assert meta_tok == "ACCOUNT", "cloud metadata must use the account token"


def test_pms_url_still_uses_the_server_token(plex) -> None:
    c, calls = plex
    c.configure_plex_context(baseurl=PMS, token="PMS-TOKEN", account_token="ACCOUNT")

    c.hydrate_external_ids("PMS-TOKEN", "1234")

    pms_tok = next(t for u, t in calls if u.startswith(PMS))
    assert pms_tok == "PMS-TOKEN"


def test_falls_back_to_the_given_token_when_no_account_token(plex) -> None:
    c, calls = plex
    c._PLEX_CTX["account_token"] = None
    c.configure_plex_context(baseurl=PMS, token="ACCOUNT")

    ids = c.hydrate_external_ids("ACCOUNT", "1234")

    assert ids == {"tmdb": "603"}
    assert all(t == "ACCOUNT" for _, t in calls)


def test_account_token_is_kept_across_home_user_switches(plex) -> None:
    c, _ = plex
    c.configure_plex_context(baseurl=PMS, token="PMS-TOKEN", account_token="ACCOUNT")

    c.configure_plex_context(baseurl=PMS, token="HOME-USER-TOKEN")

    assert c._PLEX_CTX["token"] == "HOME-USER-TOKEN"
    assert c._PLEX_CTX["account_token"] == "ACCOUNT"
