# CrossWatch test scripts
from __future__ import annotations

import copy
from typing import Any

import pytest

from cw_platform.provider_instances import build_provider_config_view, resolve_provider_block


PROFILE = "P01"

PROVIDER_BLOCKS: dict[str, dict[str, Any]] = {
    "anilist": {"access_token": "T"},
    "crosswatch": {"connected": True, "root_dir": "/config/.cw_provider/profiles/P01"},
    "floppy": {"server_url": "http://floppy:8080", "api_token": "T"},
    "kodi": {"server": "http://kodi:8080", "connection_verified": True},
    "mdblist": {"api_key": "K"},
    "nuvio": {"access_token": "T", "refresh_token": "R", "profile_id": 2},
    "publicmetadb": {"api_key": "K"},
    "punchplay": {"access_token": "T"},
    "scrob": {"server_url": "http://scrob:7330", "api_key": "K", "username": "u", "password": "p"},
    "stremio": {"auth_key": "K"},
    "tautulli": {"server_url": "http://tautulli:8181", "api_key": "K"},
}


def _profile_only_cfg(provider: str) -> dict[str, Any]:
    return {provider: {"instances": {PROFILE: dict(PROVIDER_BLOCKS[provider])}}}


@pytest.mark.parametrize("provider", sorted(PROVIDER_BLOCKS))
def test_resolve_provider_block_survives_a_narrowed_config_view(provider: str) -> None:
    cfg = _profile_only_cfg(provider)
    view = build_provider_config_view(cfg, provider, PROFILE)

    resolved = resolve_provider_block(view, provider, PROFILE)

    assert resolved
    for key, value in PROVIDER_BLOCKS[provider].items():
        assert resolved.get(key) == value


@pytest.mark.parametrize("provider", sorted(PROVIDER_BLOCKS))
def test_resolve_provider_block_reads_the_profile_from_a_full_config(provider: str) -> None:
    cfg = _profile_only_cfg(provider)

    resolved = resolve_provider_block(cfg, provider, PROFILE)
    default_block = resolve_provider_block(cfg, provider, "default")

    for key, value in PROVIDER_BLOCKS[provider].items():
        assert resolved.get(key) == value
        assert default_block.get(key) is None


@pytest.mark.parametrize("provider", sorted(PROVIDER_BLOCKS))
def test_resolve_provider_block_never_falls_back_to_another_profile(provider: str) -> None:
    cfg = _profile_only_cfg(provider)
    cfg[provider].update(PROVIDER_BLOCKS[provider])

    assert resolve_provider_block(cfg, provider, "P02") == {}


def test_auth_modules_resolve_profile_credentials_from_a_narrowed_view() -> None:
    from providers.auth import _auth_KODI, _auth_MDBLIST, _auth_PUBLICMETADB, _auth_SCROB, _auth_STREMIO, _auth_TAUTULLI
    from providers.auth._auth_FLOPPY import provider_block as floppy_block

    checks = (
        (_auth_SCROB._block, "scrob", "api_key"),
        (_auth_STREMIO._block, "stremio", "auth_key"),
        (_auth_KODI._block, "kodi", "server"),
        (_auth_MDBLIST._block, "mdblist", "api_key"),
        (_auth_PUBLICMETADB._block, "publicmetadb", "api_key"),
        (_auth_TAUTULLI._block, "tautulli", "api_key"),
        (floppy_block, "floppy", "api_token"),
    )
    for resolver, provider, field in checks:
        view = build_provider_config_view(_profile_only_cfg(provider), provider, PROFILE)
        assert resolver(view, PROFILE).get(field) == PROVIDER_BLOCKS[provider][field], provider


def test_scrob_sync_signs_requests_for_a_profile_without_a_default(monkeypatch: pytest.MonkeyPatch) -> None:
    import providers.auth._auth_SCROB as scrob_auth
    from providers.sync._mod_SCROB import SCROBModule
    from providers.sync.scrob._common import PATH_NOW_PLAYING, scrob_request

    cfg = _profile_only_cfg("scrob")
    cfg["scrob"]["instances"][PROFILE].update({"access_token": "TOKEN", "expires_at": 4102444800})
    view = build_provider_config_view(cfg, "scrob", PROFILE)

    sent: list[dict[str, Any]] = []

    class FakeSession:
        headers: dict[str, str] = {}

        def request(self, method: str, url: str, **kwargs: Any) -> Any:
            sent.append({"method": method, "url": url, **kwargs})

            class _Resp:
                status_code = 200

            return _Resp()

    adapter = SCROBModule(view, instance_id=PROFILE)
    adapter.session = FakeSession()

    scrob_request(adapter, "GET", PATH_NOW_PLAYING)

    assert len(sent) == 1
    call = sent[0]
    assert call["url"] == "http://scrob:7330/history/now-playing"
    assert call["headers"]["X-Api-Key"] == "K"
    assert call["headers"]["Authorization"] == "Bearer TOKEN"
    assert scrob_auth.token_expired(scrob_auth._block(view, PROFILE)) is False


def test_scrob_scrobble_sink_uses_the_profile_server_not_the_empty_default() -> None:
    from providers.scrobble.scrob.sink import _Adapter
    from providers.sync.scrob._common import base_url, cfg_section

    cfg = _profile_only_cfg("scrob")
    cfg["scrob"].update({"server_url": "", "api_key": "", "username": "", "password": ""})

    adapter = _Adapter(cfg, PROFILE, object())

    assert cfg_section(adapter).get("api_key") == "K"
    assert base_url(cfg_section(adapter)) == "http://scrob:7330"


def test_scrob_probe_refresh_writes_the_token_to_the_probed_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.probesAPI as probes
    import providers.auth._auth_SCROB as scrob_auth

    stored: dict[str, Any] = _profile_only_cfg("scrob")

    monkeypatch.setattr(scrob_auth, "load_config", lambda: stored)
    monkeypatch.setattr(scrob_auth, "save_config", lambda cfg: stored.update(cfg))
    monkeypatch.setattr(scrob_auth, "login", lambda *a, **kw: {"access_token": "FRESH", "expires_at": 4102444800})

    view = probes._cfg_view_for(stored, "SCROB", PROFILE)
    token = scrob_auth.access_token_for(view, instance_id=PROFILE)

    assert token == "FRESH"
    assert stored["scrob"]["instances"][PROFILE]["access_token"] == "FRESH"
    assert not str(stored["scrob"].get("access_token") or "")


def test_scrob_status_probe_never_writes_credentials_into_the_default_block(monkeypatch: pytest.MonkeyPatch) -> None:
    import api.probesAPI as probes
    import providers.auth._auth_SCROB as scrob_auth

    stored: dict[str, Any] = _profile_only_cfg("scrob")
    stored["scrob"]["instances"][PROFILE].update({"access_token": "STALE", "expires_at": 1})

    monkeypatch.setattr(scrob_auth, "load_config", lambda: copy.deepcopy(stored))
    monkeypatch.setattr(scrob_auth, "save_config", lambda cfg: stored.update(copy.deepcopy(cfg)))
    monkeypatch.setattr(scrob_auth, "login", lambda *a, **kw: {"access_token": "FRESH", "expires_at": 4102444800})

    class _Ok:
        status_code = 200

        def json(self) -> dict[str, Any]:
            return {"id": 7, "username": "u"}

    monkeypatch.setattr(scrob_auth.ScrobClient, "request", lambda self, method, path, **kw: _Ok())
    probes.PROBE_DETAIL_CACHE.clear()

    ok, reason = probes._probe_scrob_detail(probes._cfg_view_for(stored, "SCROB", PROFILE), max_age_sec=0)

    assert (ok, reason) == (True, "")
    assert stored["scrob"]["instances"][PROFILE]["access_token"] == "FRESH"
    assert not str(stored["scrob"].get("access_token") or "")
    assert stored["scrob"].get("reauth_required") is not True


def test_scrob_probe_reauth_flag_lands_on_the_probed_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    import providers.auth._auth_SCROB as scrob_auth

    stored: dict[str, Any] = _profile_only_cfg("scrob")
    stored["scrob"]["instances"][PROFILE].update({"access_token": "STALE", "expires_at": 1})

    monkeypatch.setattr(scrob_auth, "load_config", lambda: copy.deepcopy(stored))
    monkeypatch.setattr(scrob_auth, "save_config", lambda cfg: stored.update(copy.deepcopy(cfg)))

    def _needs_totp(*a: Any, **kw: Any) -> dict[str, Any]:
        raise scrob_auth.ScrobAuthError("2fa", reason="totp_required")

    monkeypatch.setattr(scrob_auth, "login", _needs_totp)

    with pytest.raises(scrob_auth.ScrobAuthError):
        scrob_auth.refresh_token({}, instance_id=PROFILE)

    assert stored["scrob"]["instances"][PROFILE]["reauth_required"] is True
    assert stored["scrob"].get("reauth_required") is not True
