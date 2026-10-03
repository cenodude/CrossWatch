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
