from __future__ import annotations

from typing import Any

PROFILE_ID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
INSTANCE_UID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def _cfg(*, profile: bool, whitelist: list[str] | None, assign: bool = True) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    if whitelist is not None:
        filters["username_whitelist"] = whitelist
    cfg: dict[str, Any] = {
        "plex": {"server_url": "http://plex:32400", "account_token": "t", "instances": {}},
        "trakt": {"client_id": "c", "client_secret": "s", "instances": {}},
        "provider_instance_ids": {INSTANCE_UID: {"provider": "PLEX", "instance": "default"}},
        "user_profiles": {},
        "scrobble": {
            "watch": {
                "routes": [
                    {
                        "id": "R1",
                        "enabled": True,
                        "provider": "plex",
                        "provider_instance": "default",
                        "sink": "trakt",
                        "sink_instance": "default",
                        "filters": filters,
                    }
                ]
            }
        },
    }
    if profile:
        cfg["user_profiles"] = {PROFILE_ID: {"label": "Alice", "instance_uids": [INSTANCE_UID]}}
        if assign:
            cfg["scrobble"]["watch"]["routes"][0]["profile_id"] = PROFILE_ID
    return cfg


def _event(account: str):
    from providers.scrobble.scrobble import ScrobbleEvent

    return ScrobbleEvent(
        action="start",
        media_type="movie",
        ids={"imdb": "tt1"},
        title="Movie",
        year=2020,
        season=None,
        number=None,
        progress=10.0,
        account=account,
        server_uuid=None,
        session_key="s1",
        raw={},
    )


def _passes(cfg: dict[str, Any], account: str) -> bool:
    from providers.scrobble.routes import build_route_cfg
    from providers.scrobble.scrobble import Dispatcher

    route = (cfg["scrobble"]["watch"]["routes"])[0]
    route_cfg = build_route_cfg(cfg, route)
    return Dispatcher([], cfg_provider=lambda: route_cfg)._passes_filters(_event(account), route_cfg)


def test_route_cfg_carries_assigned_and_effective_profile_ids() -> None:
    from providers.scrobble.routes import build_route_cfg

    cfg = _cfg(profile=True, whitelist=None)
    built = build_route_cfg(cfg, cfg["scrobble"]["watch"]["routes"][0])
    assert built["scrobble"]["watch"]["route_profile_id"] == PROFILE_ID
    assert built["scrobble"]["watch"]["route_effective_profile_id"] == PROFILE_ID

    cfg_derived = _cfg(profile=True, whitelist=None, assign=False)
    built_derived = build_route_cfg(cfg_derived, cfg_derived["scrobble"]["watch"]["routes"][0])
    assert built_derived["scrobble"]["watch"]["route_profile_id"] == ""
    assert built_derived["scrobble"]["watch"]["route_effective_profile_id"] == PROFILE_ID

    cfg_plain = _cfg(profile=False, whitelist=None)
    built_plain = build_route_cfg(cfg_plain, cfg_plain["scrobble"]["watch"]["routes"][0])
    assert built_plain["scrobble"]["watch"]["route_profile_id"] == ""
    assert built_plain["scrobble"]["watch"]["route_effective_profile_id"] == ""


def test_unscoped_route_without_whitelist_still_accepts_every_account() -> None:
    assert _passes(_cfg(profile=False, whitelist=None), "kid") is True
    assert _passes(_cfg(profile=False, whitelist=[]), "kid") is True


def test_owned_instance_alone_never_blocks_scrobbling() -> None:
    assert _passes(_cfg(profile=True, whitelist=None, assign=False), "dad") is True
    assert _passes(_cfg(profile=True, whitelist=[], assign=False), "dad") is True


def test_assigned_route_without_whitelist_blocks_every_account() -> None:
    assert _passes(_cfg(profile=True, whitelist=None), "dad") is False
    assert _passes(_cfg(profile=True, whitelist=[]), "dad") is False
    assert _passes(_cfg(profile=True, whitelist=["   "]), "dad") is False


def test_profile_scoped_route_with_whitelist_still_matches_normally() -> None:
    cfg = _cfg(profile=True, whitelist=["dad"])
    assert _passes(cfg, "dad") is True
    assert _passes(cfg, "kid") is False


def test_media_account_allowed_default_allow_flag() -> None:
    from cw_platform.account_match import media_account_allowed

    assert media_account_allowed([], "kid") is True
    assert media_account_allowed([], "kid", default_allow=False) is False
    assert media_account_allowed(["dad"], "dad", default_allow=False) is True
    assert media_account_allowed(["dad"], "kid", default_allow=False) is False


def test_route_needs_account_filter_flag() -> None:
    from providers.scrobble.routes import route_needs_account_filter

    scoped = _cfg(profile=True, whitelist=None)
    assert route_needs_account_filter(scoped, scoped["scrobble"]["watch"]["routes"][0]) is True

    scoped_ok = _cfg(profile=True, whitelist=["dad"])
    assert route_needs_account_filter(scoped_ok, scoped_ok["scrobble"]["watch"]["routes"][0]) is False

    derived = _cfg(profile=True, whitelist=None, assign=False)
    assert route_needs_account_filter(derived, derived["scrobble"]["watch"]["routes"][0]) is False

    plain = _cfg(profile=False, whitelist=None)
    assert route_needs_account_filter(plain, plain["scrobble"]["watch"]["routes"][0]) is False


def test_overview_route_row_exposes_flag() -> None:
    from api.scrobblerManagementAPI import _normalized_routes

    cfg = _cfg(profile=True, whitelist=None)
    rows = _normalized_routes(cfg, None)
    assert rows and rows[0]["needs_account_filter"] is True


def _webhook_cfg(*, profile: bool, whitelist: list[str] | None) -> dict[str, Any]:
    wh: dict[str, Any] = {
        "sinks": ["trakt"],
        "sink_instances": {"trakt": "default"},
        "filters_emby": {"username_whitelist": whitelist if whitelist is not None else []},
        "filters_jellyfin": {"username_whitelist": whitelist if whitelist is not None else []},
        "filters_plex": {"username_whitelist": whitelist if whitelist is not None else []},
    }
    if profile:
        wh["user_profile_assignments"] = {"emby:default:trakt:default": PROFILE_ID, "jellyfin:default:trakt:default": PROFILE_ID, "plex:default:trakt:default": PROFILE_ID}
    return {
        "user_profiles": {PROFILE_ID: {"label": "Alice", "instance_uids": [INSTANCE_UID]}} if profile else {},
        "provider_instance_ids": {INSTANCE_UID: {"provider": "EMBY", "instance": "default"}},
        "scrobble": {"sources": {"webhook": True}, "webhook": wh},
    }


def test_webhook_profile_scoped_detection() -> None:
    from providers.webhooks.config import profile_scoped_webhook

    assert profile_scoped_webhook(_webhook_cfg(profile=True, whitelist=None), "emby", "default") is True
    assert profile_scoped_webhook(_webhook_cfg(profile=False, whitelist=None), "emby", "default") is False
    assert profile_scoped_webhook(_webhook_cfg(profile=True, whitelist=None), "jellyfin", "default") is True
    assert profile_scoped_webhook(_webhook_cfg(profile=True, whitelist=None), "plex", "default") is True


def test_webhook_profile_scoped_detection_ignores_unassigned_sink() -> None:
    from providers.webhooks.config import profile_scoped_webhook

    cfg = _webhook_cfg(profile=True, whitelist=None)
    cfg["scrobble"]["webhook"]["sink_instances"] = {"trakt": "alt"}
    assert profile_scoped_webhook(cfg, "emby", "default") is False


def _stream(account: str) -> dict[str, Any]:
    return {"source": "plex", "provider_instance": "default", "account": account, "title": "Movie"}


def _card_filter(cfg: dict[str, Any]) -> dict[str, Any]:
    from cw_platform.access_policy import media_account_allowlist_for_profile

    return {"instances": {"PLEX": ["default"]}, "accounts": media_account_allowlist_for_profile(cfg, PROFILE_ID)}


def test_card_visible_when_owned_route_has_no_whitelist() -> None:
    from api.scrobbleAPI import _currently_watching_matches_user

    for cfg in (_cfg(profile=True, whitelist=None, assign=False), _cfg(profile=True, whitelist=[], assign=False), _cfg(profile=True, whitelist=None)):
        assert _currently_watching_matches_user(_stream("dad"), _card_filter(cfg)) is True


def test_card_scoped_by_route_whitelist() -> None:
    from api.scrobbleAPI import _currently_watching_matches_user

    cfg = _cfg(profile=True, whitelist=["dad"])
    assert _currently_watching_matches_user(_stream("dad"), _card_filter(cfg)) is True
    assert _currently_watching_matches_user(_stream("kid"), _card_filter(cfg)) is False


def test_card_ignores_streams_from_unowned_instance() -> None:
    from api.scrobbleAPI import _currently_watching_matches_user

    cfg = _cfg(profile=True, whitelist=["dad"])
    stream = dict(_stream("dad"), provider_instance="other")
    assert _currently_watching_matches_user(stream, _card_filter(cfg)) is False
