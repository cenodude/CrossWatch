from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path

from cw_platform.orchestrator._state_store import StateStore
from services import support

REPO = Path(__file__).resolve().parents[1]
SUPPORT_JS = REPO / "assets" / "js" / "modals" / "support" / "index.js"


def test_support_download_does_not_reuse_the_metadata_timeout() -> None:
    js = SUPPORT_JS.read_text(encoding="utf-8")

    fblob = js.split("async function fblob(")[1].split("\n}")[0]
    assert "DOWNLOAD_TIMEOUT_MS" in fblob
    assert "REQUEST_TIMEOUT_MS" not in fblob

    download_match = re.search(r"DOWNLOAD_TIMEOUT_MS\s*=\s*([\d_]+)", js)
    request_match = re.search(r"REQUEST_TIMEOUT_MS\s*=\s*([\d_]+)", js)
    assert download_match is not None
    assert request_match is not None
    download_ms = int(download_match.group(1).replace("_", ""))
    request_ms = int(request_match.group(1).replace("_", ""))
    assert download_ms > request_ms


def test_support_aborts_carry_a_reason_so_the_ui_never_says_user_aborted() -> None:
    js = SUPPORT_JS.read_text(encoding="utf-8")

    assert "ctrl.abort()" not in js
    for call in re.findall(r"ctrl\.abort\((.*?)\), \w+_TIMEOUT_MS\)", js):
        assert call.strip().startswith("timeoutError(")


def _baseline(items: dict) -> dict:
    return {"baseline": {"items": items}}


def _movie(title: str, imdb: str, **extra) -> dict:
    return {"type": "movie", "title": title, "ids": {"imdb": imdb}, **extra}


def _setup(tmp_path, monkeypatch) -> dict:
    state = {
        "providers": {
            "PLEX": {
                "watchlist": _baseline({"imdb:tt1": _movie("One", "tt1")}),
                "history": _baseline({"imdb:tt2": _movie("Two", "tt2", watched_at="2026-01-02T03:04:05Z")}),
                "instances": {
                    "p2": {"watchlist": _baseline({"imdb:tt3": _movie("Three", "tt3")})},
                },
            },
            "TRAKT": {
                "watchlist": _baseline({"imdb:tt1": _movie("One", "tt1")}),
                "history": _baseline({"imdb:tt4": _movie("Four", "tt4", watched_at=1700000000000)}),
            },
            "SIMKL": {
                "ratings": _baseline({"imdb:tt5": _movie("Five", "tt5")}),
            },
        },
        "last_sync_epoch": 1700000000,
    }
    StateStore(tmp_path).save_state(state)

    cfg = {
        "pairs": [
            {
                "id": "pair-main",
                "source": "PLEX",
                "source_instance": "default",
                "target": "TRAKT",
                "target_instance": "default",
                "mode": "one-way",
                "enabled": True,
                "features": {
                    "watchlist": {"enable": True},
                    "history": {"enable": True},
                    "ratings": {"enable": False},
                },
            },
            {
                "id": "pair-second",
                "source": "PLEX",
                "source_instance": "p2",
                "target": "TRAKT",
                "target_instance": "default",
                "mode": "one-way",
                "enabled": True,
                "features": {"watchlist": {"enable": True}},
            },
        ],
    }
    monkeypatch.setattr(support, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(support, "load_config", lambda: cfg)
    return cfg


def test_list_scopes_reports_pairs_and_unreferenced_baselines(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)

    scopes = support.list_scopes()
    pairs = {p["id"]: p for p in scopes["pairs"]}

    assert scopes["ok"] is True
    assert pairs["pair-main"]["features"] == ["history", "watchlist"]
    assert pairs["pair-main"]["baselines"] == 4
    assert pairs["pair-main"]["items"] == 4
    assert pairs["pair-second"]["baselines"] == 2
    assert pairs["pair-second"]["items"] == 2

    assert [(row["provider"], row["feature"]) for row in scopes["orphans"]] == [("SIMKL", "ratings")]
    assert scopes["totals"] == {
        "pairs": 2,
        "baselines": 6,
        "items": 6,
        "orphan_baselines": 1,
        "orphan_items": 1,
    }


def test_build_state_without_scope_returns_every_provider(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)

    result = support.build_state(None)
    payload = result["payload"]

    assert result["meta"]["scope"] == "all"
    assert set(payload["providers"]) == {"PLEX", "TRAKT", "SIMKL"}
    assert payload["last_sync_epoch"] == 1700000000
    assert result["meta"]["totals"] == {"providers": 3, "baselines": 6, "items": 6}


def test_build_state_scoped_to_pair_drops_other_providers_and_features(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)

    payload = support.build_state(["pair-main"])["payload"]
    providers = payload["providers"]

    assert set(providers) == {"PLEX", "TRAKT"}
    assert set(providers["PLEX"]) == {"watchlist", "history"}
    assert "instances" not in providers["PLEX"]
    assert set(providers["TRAKT"]) == {"watchlist", "history"}
    assert [item["title"] for item in payload["wall"]] == ["One"]


def test_build_state_scoped_to_instance_pair_keeps_only_that_instance(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)

    providers = support.build_state(["pair-second"])["payload"]["providers"]

    assert set(providers["PLEX"]) == {"instances"}
    assert set(providers["PLEX"]["instances"]) == {"p2"}
    assert set(providers["PLEX"]["instances"]["p2"]) == {"watchlist"}
    assert set(providers["TRAKT"]) == {"watchlist"}


def test_build_state_reports_unknown_pair_ids(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)

    meta = support.build_state(["pair-main", "pair-gone"])["meta"]

    assert meta["unknown_pair_ids"] == ["pair-gone"]
    assert meta["pair_ids"] == ["pair-main"]


def test_state_summary_flags_non_iso_timestamps(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)

    summary = support._state_summary(support.build_state(None)["payload"])
    trakt_history = next(
        row for row in summary["baselines"] if row["provider"] == "TRAKT" and row["feature"] == "history"
    )
    plex_history = next(
        row for row in summary["baselines"] if row["provider"] == "PLEX" and row["feature"] == "history"
    )

    assert trakt_history["timestamp_issues"] == {"watched_at:epoch_ms_string": 1}
    assert trakt_history["id_coverage"] == {"imdb": 1}
    assert plex_history["timestamp_issues"] == {}


def test_bundle_contains_state_and_selected_sections(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)

    archive = zipfile.ZipFile(io.BytesIO(support.build_bundle(["pair-main"], ["config"])))
    names = set(archive.namelist())
    manifest = json.loads(archive.read("manifest.json"))
    state = json.loads(archive.read("state.json"))

    assert names == {"state.json", "pairs.json", "config.redacted.json", "manifest.json"}
    assert manifest["sections"] == ["config"]
    assert manifest["scope"] == "pairs"
    assert manifest["pair_ids"] == ["pair-main"]
    assert set(state["providers"]) == {"PLEX", "TRAKT"}


def test_bundle_without_sections_still_carries_state(tmp_path, monkeypatch) -> None:
    _setup(tmp_path, monkeypatch)

    archive = zipfile.ZipFile(io.BytesIO(support.build_bundle(None, ["none"])))

    assert set(archive.namelist()) == {"state.json", "pairs.json", "manifest.json"}
    assert json.loads(archive.read("manifest.json"))["sections"] == []


def test_bundle_masks_config_secrets(tmp_path, monkeypatch) -> None:
    cfg = _setup(tmp_path, monkeypatch)
    cfg["trakt"] = {"access_token": "super-secret-token", "client_id": "public-id"}

    archive = zipfile.ZipFile(io.BytesIO(support.build_bundle(None, ["config"])))
    redacted = json.loads(archive.read("config.redacted.json"))

    assert redacted["trakt"]["access_token"] != "super-secret-token"
    assert redacted["trakt"]["client_id"] == "<redacted>"


def test_bundle_masks_managed_users_totp_and_oidc(tmp_path, monkeypatch) -> None:
    cfg = _setup(tmp_path, monkeypatch)
    cfg["app_auth"] = {
        "enabled": True,
        "username": "admin-user",
        "password": {"salt": "admin-salt", "hash": "admin-hash"},
        "session": {"token_hash": "admin-session-token", "expires_at": 1},
        "sessions": [
            {
                "token_hash": "managed-session-token",
                "ua": "managed-browser",
                "ip": "192.168.1.5",
                "user_id": "user-one",
            }
        ],
        "totp": {"enabled": True, "secret": "ADMIN-TOTP", "pending_secret": "ADMIN-PENDING"},
        "recovery_codes": [{"hash": "admin-recovery-hash", "used_at": 0}],
        "oidc": {
            "enabled": True,
            "issuer": "https://issuer.example",
            "client_id": "oidc-client",
            "client_secret": "oidc-secret",
            "scopes": "openid profile email",
        },
        "oidc_identity": {
            "iss": "https://issuer.example",
            "sub": "admin-sub",
            "username": "admin-oidc",
            "email": "admin@example.com",
            "picture": "https://issuer.example/admin.png",
            "linked_at": 1,
        },
        "users": {
            "user-one": {
                "username": "managed-user",
                "display_name": "Managed Real Name",
                "profile_id": "profile-one",
                "password": {"salt": "user-salt", "hash": "user-hash"},
                "totp": {"enabled": True, "secret": "USER-TOTP", "pending_secret": "USER-PENDING"},
                "recovery_codes": [{"hash": "user-recovery-hash", "used_at": 0}],
                "avatar": {
                    "file": "abc123456789abc123456789abc12345.png",
                    "content_type": "image/png",
                    "updated_at": 1,
                },
                "oidc": {
                    "iss": "https://issuer.example",
                    "sub": "managed-sub",
                    "username": "managed-oidc",
                    "email": "managed@example.com",
                    "picture": "https://issuer.example/managed.png",
                    "linked_at": 1,
                },
                "plex_sso": {
                    "account_id": "plex-account",
                    "username": "plex-user",
                    "email": "plex@example.com",
                    "thumb": "https://plex.example/thumb.png",
                    "linked_at": 1,
                },
            }
        },
    }
    cfg["pairs"][0]["profile_id"] = "profile-one"

    archive = zipfile.ZipFile(io.BytesIO(support.build_bundle(None, ["config"])))
    redacted = json.loads(archive.read("config.redacted.json"))
    pairs = json.loads(archive.read("pairs.json"))
    auth = redacted["app_auth"]
    managed = auth["users"]["user-one"]
    text = json.dumps({"config": redacted, "pairs": pairs})

    for secret in (
        "admin-user",
        "admin-salt",
        "admin-hash",
        "admin-session-token",
        "managed-session-token",
        "managed-browser",
        "192.168.1.5",
        "ADMIN-TOTP",
        "ADMIN-PENDING",
        "admin-recovery-hash",
        "https://issuer.example",
        "oidc-client",
        "oidc-secret",
        "admin-sub",
        "admin-oidc",
        "admin@example.com",
        "managed-user",
        "Managed Real Name",
        "profile-one",
        "user-salt",
        "user-hash",
        "USER-TOTP",
        "USER-PENDING",
        "user-recovery-hash",
        "abc123456789abc123456789abc12345.png",
        "managed-sub",
        "managed-oidc",
        "managed@example.com",
        "plex-account",
        "plex-user",
        "plex@example.com",
    ):
        assert secret not in text

    assert auth["username"] == "<redacted>"
    assert auth["session"]["token_hash"] == "<redacted>"
    assert auth["sessions"] == "<1 item>"
    assert auth["totp"]["secret"] == "<redacted>"
    assert auth["recovery_codes"] == "<1 item>"
    assert auth["oidc"]["client_secret"] == "<redacted>"
    assert auth["oidc_identity"]["sub"] == "<redacted>"
    assert managed["display_name"] == "<redacted>"
    assert managed["totp"]["pending_secret"] == "<redacted>"
    assert managed["recovery_codes"] == "<1 item>"
    assert managed["avatar"]["file"] == "<redacted>"
    assert managed["oidc"]["email"] == "<redacted>"
    assert managed["plex_sso"]["account_id"] == "<redacted>"
    assert pairs["pairs"][0]["profile_id"] == "<redacted>"
