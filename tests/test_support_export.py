from __future__ import annotations

import io
import json
import zipfile

from cw_platform.orchestrator._state_store import StateStore
from services import support


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
    assert redacted["trakt"]["client_id"] == "public-id"
