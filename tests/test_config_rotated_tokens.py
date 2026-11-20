# CrossWatch test scripts
from __future__ import annotations

import copy

from cw_platform import config_base as cb


def _rotate(disk: dict, incoming: dict) -> tuple[dict, list[str]]:
    data = copy.deepcopy(incoming)
    kept = cb._preserve_rotated_tokens(data, copy.deepcopy(disk))
    return data, kept


def test_legitimate_rotation_is_written_and_recorded() -> None:
    disk = {"punchplay": {"access_token": "at-1", "refresh_token": "rt-1", "expires_at": 100}}
    incoming = {"punchplay": {"access_token": "at-2", "refresh_token": "rt-2", "expires_at": 50}}

    data, kept = _rotate(disk, incoming)

    assert kept == []
    assert data["punchplay"]["refresh_token"] == "rt-2"
    assert data["punchplay"][cb._TOKEN_LINEAGE_KEY] == [cb._token_digest("rt-1")]


def test_stale_snapshot_cannot_write_back_a_rotated_refresh_token() -> None:
    lineage = [cb._token_digest("rt-1")]
    disk = {"punchplay": {"access_token": "at-2", "refresh_token": "rt-2", "expires_at": 200, cb._TOKEN_LINEAGE_KEY: lineage}}
    incoming = {"punchplay": {"access_token": "at-1", "refresh_token": "rt-1", "expires_at": 100, "timeout": 30}}

    data, kept = _rotate(disk, incoming)

    assert kept == ["punchplay:default"]
    blk = data["punchplay"]
    assert (blk["access_token"], blk["refresh_token"], blk["expires_at"]) == ("at-2", "rt-2", 200)
    assert blk[cb._TOKEN_LINEAGE_KEY] == lineage
    assert blk["timeout"] == 30


def test_stale_snapshot_is_caught_per_instance() -> None:
    lineage = [cb._token_digest("rt-a1")]
    disk = {"trakt": {"instances": {"P1": {"refresh_token": "rt-a2", "access_token": "at-a2", cb._TOKEN_LINEAGE_KEY: lineage}}}}
    incoming = {"trakt": {"instances": {"P1": {"refresh_token": "rt-a1", "access_token": "at-a1"}}}}

    data, kept = _rotate(disk, incoming)

    assert kept == ["trakt:P1"]
    assert data["trakt"]["instances"]["P1"]["refresh_token"] == "rt-a2"


def test_unchanged_token_keeps_lineage_from_disk() -> None:
    lineage = [cb._token_digest("rt-1")]
    disk = {"simkl": {"refresh_token": "rt-2", cb._TOKEN_LINEAGE_KEY: lineage}}
    incoming = {"simkl": {"refresh_token": "rt-2"}}

    data, kept = _rotate(disk, incoming)

    assert kept == []
    assert data["simkl"][cb._TOKEN_LINEAGE_KEY] == lineage


def test_disconnect_and_reconnect_are_not_blocked() -> None:
    lineage = [cb._token_digest("rt-1")]
    disk = {"punchplay": {"access_token": "at-2", "refresh_token": "rt-2", cb._TOKEN_LINEAGE_KEY: lineage}}

    cleared, kept = _rotate(disk, {"punchplay": {"access_token": "", "refresh_token": ""}})
    assert kept == []
    assert cleared["punchplay"]["refresh_token"] == ""

    fresh, kept = _rotate(disk, {"punchplay": {"access_token": "at-new", "refresh_token": "rt-new"}})
    assert kept == []
    assert fresh["punchplay"]["refresh_token"] == "rt-new"


def test_lineage_is_bounded() -> None:
    lineage = [cb._token_digest(f"rt-{i}") for i in range(cb._TOKEN_LINEAGE_MAX)]
    disk = {"punchplay": {"refresh_token": "rt-cur", cb._TOKEN_LINEAGE_KEY: lineage}}

    data, _ = _rotate(disk, {"punchplay": {"refresh_token": "rt-next"}})

    assert len(data["punchplay"][cb._TOKEN_LINEAGE_KEY]) == cb._TOKEN_LINEAGE_MAX
    assert data["punchplay"][cb._TOKEN_LINEAGE_KEY][0] == cb._token_digest("rt-cur")


def test_providers_without_rotation_are_untouched() -> None:
    disk = {"plex": {"refresh_token": "a"}}
    data, kept = _rotate(disk, {"plex": {"refresh_token": "b"}})

    assert kept == []
    assert cb._TOKEN_LINEAGE_KEY not in data["plex"]


def test_silo_connection_is_guarded_and_keeps_its_session_stamp() -> None:
    lineage = [cb._token_digest("rt-1")]
    disk = {"silo": {"access_token": "at-2", "refresh_token": "rt-2", "expires_at": 200, "session_refreshed_at": 150, cb._TOKEN_LINEAGE_KEY: lineage}}
    incoming = {"silo": {"access_token": "at-1", "refresh_token": "rt-1", "expires_at": 100, "session_refreshed_at": 90, "profile_id": "p-1"}}

    data, kept = _rotate(disk, incoming)

    assert kept == ["silo:default"]
    blk = data["silo"]
    assert (blk["refresh_token"], blk["expires_at"], blk["session_refreshed_at"], blk["profile_id"]) == ("rt-2", 200, 150, "p-1")


def test_stale_write_cannot_roll_back_an_identity_session() -> None:
    lineage = [cb._token_digest("acc-rt-1")]
    fresh = {"access_token": "acc-at-2", "refresh_token": "acc-rt-2", "expires_at": 300, "account_user_id": "2", cb._TOKEN_LINEAGE_KEY: lineage}
    stale = {"access_token": "acc-at-1", "refresh_token": "acc-rt-1", "expires_at": 100, "account_user_id": "2", "username": "Other"}
    disk = {"silo": {"refresh_token": "rt-p", "sync_users": {"p-other": fresh},
                     "instances": {"S2": {"refresh_token": "rt-s2", "sync_users": {"p-x": dict(fresh)}}}}}
    incoming = {"silo": {"refresh_token": "rt-p", "sync_users": {"p-other": dict(stale)},
                         "instances": {"S2": {"refresh_token": "rt-s2", "sync_users": {"p-x": dict(stale)}}}}}

    data, kept = _rotate(disk, incoming)

    assert sorted(kept) == ["silo:S2~p-x", "silo:default~p-other"]
    for entry in (data["silo"]["sync_users"]["p-other"], data["silo"]["instances"]["S2"]["sync_users"]["p-x"]):
        assert (entry["access_token"], entry["refresh_token"], entry["expires_at"], entry["username"]) == ("acc-at-2", "acc-rt-2", 300, "Other")


def test_identity_session_end_and_reopen_are_not_blocked() -> None:
    lineage = [cb._token_digest("acc-rt-1")]
    disk = {"silo": {"sync_users": {"p-other": {"access_token": "acc-at-2", "refresh_token": "acc-rt-2", cb._TOKEN_LINEAGE_KEY: lineage}}}}

    ended, kept = _rotate(disk, {"silo": {"sync_users": {"p-other": {"access_token": "", "refresh_token": "", "auth_error": "reconnect_required"}}}})
    assert kept == [] and ended["silo"]["sync_users"]["p-other"]["refresh_token"] == ""

    reopened, kept = _rotate(disk, {"silo": {"sync_users": {"p-other": {"access_token": "new-at", "refresh_token": "new-rt"}}}})
    assert kept == [] and reopened["silo"]["sync_users"]["p-other"]["refresh_token"] == "new-rt"
    assert reopened["silo"]["sync_users"]["p-other"][cb._TOKEN_LINEAGE_KEY][0] == cb._token_digest("acc-rt-2")


def test_household_identities_without_tokens_are_untouched() -> None:
    disk = {"silo": {"refresh_token": "rt-p", "sync_users": {"p-kid": {"profile_token": "kid-1"}}}}
    data, kept = _rotate(disk, {"silo": {"refresh_token": "rt-p", "sync_users": {"p-kid": {"profile_token": "kid-2"}}}})

    assert kept == []
    assert data["silo"]["sync_users"]["p-kid"] == {"profile_token": "kid-2"}


def test_stale_write_cannot_bring_back_a_disconnected_session() -> None:
    disk = {"silo": {"access_token": "at-2", "refresh_token": "rt-2", "expires_at": 200,
                     "sync_users": {"p-other": {"access_token": "acc-at", "refresh_token": "acc-rt", "account_user_id": "2"}}}}
    cleared, _ = _rotate(disk, {"silo": {"access_token": "", "refresh_token": "", "expires_at": 0,
                                         "sync_users": {"p-other": {"access_token": "", "refresh_token": "", "account_user_id": "2",
                                                                    "auth_error": "reconnect_required"}}}})
    assert cleared["silo"][cb._TOKEN_LINEAGE_KEY] == [cb._token_digest("rt-2")]

    late, kept = _rotate(cleared, copy.deepcopy(disk))

    assert sorted(kept) == ["silo:default", "silo:default~p-other"]
    assert late["silo"]["refresh_token"] == "" and late["silo"]["access_token"] == ""
    entry = late["silo"]["sync_users"]["p-other"]
    assert entry["refresh_token"] == "" and entry["auth_error"] == "reconnect_required"

    relogin, kept = _rotate(cleared, {"silo": {"access_token": "at-new", "refresh_token": "rt-new", "expires_at": 300}})
    assert kept == [] and relogin["silo"]["refresh_token"] == "rt-new"
    assert relogin["silo"][cb._TOKEN_LINEAGE_KEY] == [cb._token_digest("rt-2")]

    after, kept = _rotate(relogin, copy.deepcopy(disk))
    assert "silo:default" in kept and after["silo"]["refresh_token"] == "rt-new"


def test_cleared_block_without_history_stays_plain() -> None:
    data, kept = _rotate({"plex": {}, "trakt": {"refresh_token": ""}}, {"trakt": {"refresh_token": "", "access_token": ""}})

    assert kept == [] and cb._TOKEN_LINEAGE_KEY not in data["trakt"]


def test_saved_stale_copy_never_wipes_a_later_login(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(cb, "CONFIG", tmp_path)
    cfg = cb.load_config()
    cfg["silo"].update({"server_url": "http://silo", "access_token": "at-1", "refresh_token": "rt-1", "expires_at": 100})
    cb.save_config(cfg)
    inflight = cb.load_config()

    off = cb.load_config()
    off["silo"].update({"access_token": "", "refresh_token": "", "expires_at": 0})
    cb.save_config(off)
    cb.save_config(inflight)
    assert cb.load_config()["silo"]["refresh_token"] == ""
    assert inflight["silo"]["refresh_token"] == "rt-1"

    login = cb.load_config()
    login["silo"].update({"access_token": "at-new", "refresh_token": "rt-new", "expires_at": 300})
    cb.save_config(login)
    cb.save_config(inflight)
    assert cb.load_config()["silo"]["refresh_token"] == "rt-new"
