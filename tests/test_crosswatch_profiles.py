# tests/test_crosswatch_profiles.py
# CrossWatch - Local tracker profile tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from pathlib import Path
from typing import Any
import json

import api.providerInstancesAPI as provider_api
import api.editorAPI as editor_api
import api.insightAPI as insight_api
import api.authenticationAPI as auth_api
import services.editor as editor_service
import services.export as export_service
from cw_platform.provider_instances import build_pair_config_view, get_provider_block
from providers.sync._mod_CROSSWATCH import CROSSWATCHModule


def test_crosswatch_next_profile_uses_cw_prefix_and_label(monkeypatch) -> None:
    store: dict[str, Any] = {"crosswatch": {"root_dir": "/config/.cw_provider"}}

    def fake_load() -> dict[str, Any]:
        return store

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "_invalidate_provider_cache", lambda provider: None)

    res = provider_api.api_provider_instances_create_next("crosswatch", {"label": "Living Room Tracker"})

    assert res == {"ok": True, "id": "CW-P01"}
    block = store["crosswatch"]["instances"]["CW-P01"]
    assert block["label"] == "Living Room"
    assert block["root_dir"] == "/config/.cw_provider/profiles/CW-P01"


def test_crosswatch_provider_block_derives_profile_root_when_missing(tmp_path: Path) -> None:
    root = tmp_path / "cw_provider"
    cfg = {"crosswatch": {"root_dir": str(root), "instances": {"CW-P01": {"label": "Kids"}}}}

    block = get_provider_block(cfg, "CROSSWATCH", "CW-P01")

    assert block["label"] == "Kids"
    assert block["root_dir"].replace("\\", "/").endswith("/cw_provider/profiles/CW-P01")


def test_crosswatch_pair_config_view_selects_profile_root(tmp_path: Path) -> None:
    root = tmp_path / "cw_provider"
    cfg = {
        "simkl": {"access_token": "token"},
        "crosswatch": {"root_dir": str(root), "instances": {"CW-P02": {"retention_days": 7}}},
    }

    view = build_pair_config_view(cfg, "SIMKL", "default", "CROSSWATCH", "CW-P02")

    assert view["crosswatch"]["retention_days"] == 7
    assert view["crosswatch"]["root_dir"].replace("\\", "/").endswith("/cw_provider/profiles/CW-P02")


def test_crosswatch_module_uses_profile_root_from_config_view(tmp_path: Path) -> None:
    root = tmp_path / "cw_provider"
    cfg = {"crosswatch": {"root_dir": str(root), "instances": {"CW-P03": {}}}}
    view = build_pair_config_view(cfg, "SIMKL", "default", "CROSSWATCH", "CW-P03")

    mod = CROSSWATCHModule(view)

    assert str(mod.cfg.base_path).replace("\\", "/").endswith("/cw_provider/profiles/CW-P03")


def test_editor_tracker_workspaces_select_crosswatch_profile_root(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "cw_provider"
    default_root = root
    profile_root = root / "profiles" / "CW-P01"
    default_root.mkdir(parents=True)
    profile_root.mkdir(parents=True)
    (default_root / "watchlist.json").write_text('{"items":{"movie:default":{"title":"Default"}}}', "utf-8")
    (profile_root / "watchlist.json").write_text('{"items":{"movie:p01":{"title":"P01"}}}', "utf-8")

    cfg = {"crosswatch": {"root_dir": str(root), "instances": {"CW-P01": {"label": "Desk"}}}}
    monkeypatch.setattr(editor_api, "load_config", lambda: cfg)

    workspaces = editor_api.api_editor_tracker_workspaces("CW-P01")
    loaded = editor_api.api_editor_get_state(
        kind="watchlist",
        source="tracker",
        provider_instance="CW-P01",
        workspace="default",
    )

    assert workspaces["provider_instance"] == "CW-P01"
    assert workspaces["workspaces"][0]["profile_id"] == "CW-P01"
    assert loaded["provider_instance"] == "CW-P01"
    assert set(loaded["items"].keys()) == {"movie:p01"}


def test_editor_hides_local_tracker_workspace_selector() -> None:
    editor_js = Path("assets/js/editor.js").read_text("utf-8")

    assert "function syncSnapshotControlVisibility()" in editor_js
    assert "const show = !isTrackerSource();" in editor_js
    assert "if (snapLabel) snapLabel.style.display = show ? \"\" : \"none\";" in editor_js
    assert "if (snapLabel) snapLabel.textContent = isState ? \"Provider\" : \"Endpoint\";" in editor_js
    assert 'isTracker ? "Workspace"' not in editor_js


def test_editor_tracker_manual_policy_is_stored_per_crosswatch_profile(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "cw_provider"
    profile_root = root / "profiles" / "CW-P02"
    profile_root.mkdir(parents=True)
    (profile_root / "watchlist.json").write_text('{"items":{"movie:base":{"title":"Base"}}}', "utf-8")

    monkeypatch.setattr(editor_api, "load_config", lambda: {
        "crosswatch": {"root_dir": str(root), "instances": {"CW-P02": {}}},
    })
    monkeypatch.setattr(editor_api, "_STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(editor_api, "_POLICY_PATH", tmp_path / "state.manual.json")

    res = editor_api.api_editor_save_state({
        "kind": "watchlist",
        "source": "tracker",
        "provider_instance": "CW-P02",
        "workspace": "default",
        "items": {"movie:manual": {"title": "Manual", "type": "movie"}},
        "blocks": ["movie:base"],
    })
    adds, blocks = editor_api._load_policy_manual("watchlist", "CROSSWATCH", "CW-P02")

    assert res["provider_instance"] == "CW-P02"
    assert list(adds.values()) == [{"title": "Manual", "type": "movie"}]
    assert blocks == ["movie:base"]
    assert editor_api._load_policy_manual("watchlist", "CROSSWATCH", "default") == ({}, [])


def test_tracker_archive_json_import_uses_crosswatch_profile_snapshot_dir(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "cw_provider"
    monkeypatch.setattr(editor_service, "load_config", lambda: {
        "crosswatch": {"root_dir": str(root), "instances": {"CW-P04": {"retention_days": 0}}},
    })

    stats = editor_service.import_tracker_json(
        b'{"items":{"movie:x":{"title":"X"}}}',
        "20260101T000000Z-watchlist.json",
        "CW-P04",
    )

    assert stats["target"] == "snapshot"
    assert (root / "profiles" / "CW-P04" / "snapshots" / "20260101T000000Z-watchlist.json").exists()
    assert not (root / "snapshots" / "20260101T000000Z-watchlist.json").exists()


def test_status_probes_include_crosswatch_profiles(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api import probesAPI as probes

    cfg = {
        "crosswatch": {
            "connected": True,
            "root_dir": "/config/.cw_provider",
            "instances": {
                "CW-P01": {"connected": True, "label": "Desk"},
                "CW-P02": {"connected": True, "enabled": False, "label": "Old"},
            },
        }
    }

    probes.invalidate_provider_caches("crosswatch")
    app = FastAPI()
    probes.register_probes(app, lambda: cfg)

    data = TestClient(app).get("/api/status?fresh=1").json()
    body = json.dumps(data)
    cw = data["providers"]["CROSSWATCH"]

    assert data["crosswatch_connected"] is True
    assert cw["connected"] is True
    assert cw["vip"] is True
    assert cw["vip_text"] == "You've earned it"
    assert cw["instances_summary"]["total"] == 3
    assert cw["instances"]["default"]["configured"] is True
    assert cw["instances"]["CW-P01"]["configured"] is True
    assert cw["instances"]["CW-P02"]["connected"] is False
    assert "Desk" not in body


def test_main_status_crosswatch_vip_copy() -> None:
    text = Path("assets/js/main-status.js").read_text("utf-8")
    provider_meta = Path("assets/helpers/provider-meta.js").read_text("utf-8")
    css = Path("assets/crosswatch.css").read_text("utf-8")

    assert 'case "CROSSWATCH"' in text
    assert "Plan: VIP" in text
    assert "Plan: Free" in text
    assert "Plan: Premiere" in text
    assert "You've earned it" in text
    assert "SIMKL plan:" not in text
    assert "VIP status" not in text
    assert "Free account" not in text
    assert 'info.key !== "CROSSWATCH"' not in provider_meta
    assert 'CROSSWATCH: { key: "CROSSWATCH"' in provider_meta
    assert "max-width:520px !important" in css
    assert "max-width:340px !important" not in css


def test_crosswatch_tracker_settings_live_only_in_connection_modal() -> None:
    settings_html = Path("ui_frontend.py").read_text("utf-8")
    settings_ui = Path("assets/helpers/settings-ui.js").read_text("utf-8")
    settings_save = Path("assets/helpers/settings-save.js").read_text("utf-8")
    auth_html = Path("providers/auth/_auth_CROSSWATCH.py").read_text("utf-8")
    auth_js = Path("assets/auth/auth.crosswatch.js").read_text("utf-8")
    providers_ui = Path("assets/helpers/providers-ui.js").read_text("utf-8")

    assert 'data-target="tracker"' not in settings_html
    assert 'data-tab="tracker"' not in settings_html
    assert "Retention, capture and restore snapshots" not in settings_html
    assert "Settings (UI / Security / Local Tracker)" not in settings_html
    assert "loadCrossWatchSnapshots" not in settings_ui
    assert "cw_restore_watchlist" not in settings_save

    assert "cw_tracker_label" in auth_html
    assert 'maxlength="12"' in auth_html
    assert 'data-sub="auth"' in auth_html
    assert "cw_crosswatch_connect" in auth_html
    assert "Manage Local Tracker Profiles" not in auth_html
    assert "cw-field-help material-symbols-rounded" in auth_html
    assert "cw-tracker-settings-stack" in auth_html
    assert "Local tracker storage" in auth_html
    assert "Restore snapshots" in auth_html
    assert "Local tracker label setting help" in auth_html
    assert "Local tracker progress restore help" in auth_html
    assert "/api/crosswatch/connect" in auth_js
    assert "cw_tracker_msg" not in auth_html
    assert "cw_tracker_msg" not in auth_js
    assert ">Ready<" not in auth_html
    assert 'data-sub="restore"' not in auth_html
    assert "cw_tracker_enabled" not in auth_html
    assert "cw_tracker_enabled" not in auth_js
    assert 'set("enabled"' not in settings_save
    assert "#cw_crosswatch_disconnect" in providers_ui
    assert 'restore: ["restore_page"' not in providers_ui
    assert "Local Tracker Restore" not in providers_ui
    assert "Storage and restore" in providers_ui
    assert "Create CW-P profiles when you need separate local tracker data" in providers_ui
    assert 'introSubs: ["auth"]' in providers_ui
    assert 'introSubs: ["auth", "settings"]' not in providers_ui
    assert "cw_crosswatch_disconnect" in auth_html
    assert "cw_tracker_retention_days" in auth_html
    assert "cw_tracker_auto_snapshot" in auth_html
    assert "cw_tracker_max_snapshots" in auth_html
    assert "cw_tracker_root_dir" not in auth_html
    assert "Storage root" not in auth_html
    assert "cw_tracker_root_dir" not in auth_js
    assert "cw_tracker_root_dir" not in settings_save
    assert "cw_tracker_restore_progress" in auth_html


def test_maintenance_tracker_archive_uses_profile_selector_toolbar() -> None:
    root = Path(__file__).resolve().parents[1]
    modal_js = (root / "assets" / "js" / "modals" / "maintenance" / "index.js").read_text("utf-8")
    modal_css = (root / "assets" / "js" / "modals" / "maintenance" / "styles.css").read_text("utf-8")
    icon_select_js = (root / "assets" / "helpers" / "icon-select.js").read_text("utf-8")
    profile_select_js = (root / "assets" / "helpers" / "profile-select.js").read_text("utf-8")

    assert "tracker-archive-options" in modal_js
    assert "tracker-profile-control" in modal_js
    assert "archive-btn icon-only secondary" in modal_js
    assert "aria-label=\"Download tracker archive\"" in modal_js
    assert "aria-label=\"Import tracker archive\"" in modal_js
    assert "CW?.ProfileSelect?.enhanceProfile" in modal_js
    assert "cxm-tracker-profile-select" in modal_js
    assert "menuClassName: \"cxm-tracker-profile-menu\"" in modal_js
    assert "menuMinWidth: 220" in modal_js
    assert "button, input, label, a, summary, .cw-icon-select" in modal_js
    assert "tracker-archive-options { align-items: center; flex-wrap: nowrap" in modal_css
    assert "archive-btn.icon-only" in modal_css
    assert ".tracker-profile-control .cxm-tracker-profile-select" in modal_css
    assert "cxm-tracker-profile-menu" in modal_css
    assert "menuMinWidth" in icon_select_js
    assert "menuClassName" in icon_select_js
    assert "{ ...cfg, className:" in profile_select_js


def test_crosswatch_profile_delete_removes_profile_storage(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "cw_provider"
    profile_root = root / "profiles" / "CW-P04"
    profile_root.mkdir(parents=True)
    (profile_root / "watchlist.json").write_text("{}", "utf-8")
    store: dict[str, Any] = {"crosswatch": {"root_dir": str(root), "instances": {"CW-P04": {"label": "Desk"}}}}

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "_invalidate_provider_cache", lambda provider: None)

    res = provider_api.api_provider_instances_delete("crosswatch", "CW-P04")

    assert res["ok"] is True
    assert res["storage"]["removed"] is True
    assert "CW-P04" not in store["crosswatch"]["instances"]
    assert not profile_root.exists()


def test_crosswatch_disconnect_removes_connection_and_storage(tmp_path: Path, monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    root = tmp_path / "cw_provider"
    root.mkdir()
    (root / "watchlist.json").write_text("{}", "utf-8")
    store: dict[str, Any] = {"crosswatch": {"root_dir": str(root), "retention_days": 30}}

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(auth_api, "load_config", fake_load)
    monkeypatch.setattr(auth_api, "save_config", fake_save)

    app = FastAPI()
    auth_api.register_auth(app)
    data = TestClient(app).post("/api/crosswatch/disconnect").json()

    assert data["ok"] is True
    assert data["storage"]["removed"] is True
    assert "crosswatch" not in store
    assert not root.exists()


def test_crosswatch_connect_creates_connection_config(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    store: dict[str, Any] = {}

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(auth_api, "load_config", fake_load)
    monkeypatch.setattr(auth_api, "save_config", fake_save)

    app = FastAPI()
    auth_api.register_auth(app)
    data = TestClient(app).post("/api/crosswatch/connect").json()

    assert data == {"ok": True, "instance": "default"}
    assert store["crosswatch"]["root_dir"] == "/config/.cw_provider"
    assert store["crosswatch"]["connected"] is True
    assert store["crosswatch"]["retention_days"] == 30
    assert store["crosswatch"]["auto_snapshot"] is True
    assert store["crosswatch"]["max_snapshots"] == 64


def test_crosswatch_default_config_is_not_connected() -> None:
    from providers.auth._auth_CROSSWATCH import PROVIDER
    from providers.sync._mod_CROSSWATCH import OPS

    cfg = {"crosswatch": {"root_dir": "/config/.cw_provider", "connected": False}}

    assert PROVIDER.get_status(cfg).connected is False
    assert OPS.is_configured(cfg) is False


def test_crosswatch_config_default_does_not_auto_connect(tmp_path: Path, monkeypatch) -> None:
    import cw_platform.config_base as config_base

    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    cfg = config_base.load_config()

    assert cfg["crosswatch"]["connected"] is False

    (tmp_path / "config.json").write_text(json.dumps({"crosswatch": {"root_dir": "/tmp/cw"}}), "utf-8")
    migrated = config_base.load_config()

    assert migrated["crosswatch"]["connected"] is True


def test_analyzer_treats_crosswatch_as_tracker_provider() -> None:
    import services.analyzer as analyzer

    assert analyzer._is_tracker_to_media_server("CROSSWATCH@CW-P01", ["PLEX"])


def test_profile_labels_are_used_in_analyzer_and_events_modals() -> None:
    analyzer_js = Path("assets/js/modals/analyzer/index.js").read_text("utf-8")
    events_js = Path("assets/js/modals/events/index.js").read_text("utf-8")

    assert "/api/provider-instances" in analyzer_js
    assert "row.label" in analyzer_js
    assert "/api/provider-instances" in events_js
    assert "PROFILE_LABELS" in events_js
    assert "row.label" in events_js


def test_exporter_options_label_crosswatch_profiles_from_config(tmp_path: Path, monkeypatch) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                "providers": {
                    "CROSSWATCH": {
                        "instances": {
                            "CW-P01": {
                                "history": {
                                    "baseline": {
                                        "items": {
                                            "tmdb:1": {
                                                "type": "movie",
                                                "title": "Desk Movie",
                                                "ids": {"tmdb": "1"},
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        ),
        "utf-8",
    )
    monkeypatch.setattr(export_service, "STATE_PATH", state_path)
    monkeypatch.setattr(
        export_service,
        "load_config",
        lambda: {"crosswatch": {"instances": {"CW-P01": {"label": "Desk"}}}},
    )

    opts = export_service.api_export_options()

    assert {"id": "CW-P01", "label": "CW-P01 - Desk"} in opts["instances"]["CROSSWATCH"]


def test_insights_snapshot_selector_saves_crosswatch_profile_choice(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    store: dict[str, Any] = {
        "crosswatch": {
            "root_dir": "/config/.cw_provider",
            "instances": {"CW-P01": {"label": "Desk"}},
        }
    }

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    monkeypatch.setattr(insight_api, "_env", lambda: (None, fake_load, fake_save, lambda *a, **k: None))

    app = FastAPI()
    insight_api.register_insights(app)
    res = TestClient(app).post(
        "/api/crosswatch/select-snapshot",
        params={"feature": "ratings", "snapshot": "20260101T000000Z-ratings.json", "provider_instance": "CW-P01"},
    ).json()

    assert res == {
        "ok": True,
        "feature": "ratings",
        "snapshot": "20260101T000000Z-ratings.json",
        "provider_instance": "CW-P01",
    }
    assert store["crosswatch"]["instances"]["CW-P01"]["restore_ratings"] == "20260101T000000Z-ratings.json"
    assert "restore_ratings" not in store["crosswatch"]


def test_insights_crosswatch_snapshots_include_profiles(tmp_path: Path, monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    root = tmp_path / "cw_provider"
    profile_root = root / "profiles" / "CW-P01"
    (root / "snapshots").mkdir(parents=True)
    (profile_root / "snapshots").mkdir(parents=True)
    (root / "snapshots" / "20260101T000000Z-ratings.json").write_text("{}", "utf-8")
    (profile_root / "snapshots" / "20260202T000000Z-ratings.json").write_text("{}", "utf-8")
    cfg = {
        "crosswatch": {
            "root_dir": str(root),
            "instances": {"CW-P01": {"label": "Desk", "restore_ratings": "20260202T000000Z-ratings.json"}},
        }
    }

    monkeypatch.setattr(insight_api, "_env", lambda: (None, lambda: cfg, lambda _cfg: None, lambda *a, **k: None))

    app = FastAPI()
    insight_api.register_insights(app)
    snapshots = TestClient(app).get("/api/insights?limit_samples=0&history=0").json()["crosswatch_snapshots"]

    profile_rows = {row["id"]: row for row in snapshots["_profiles"]}
    assert profile_rows["CW-P01"]["label"] == "CW-P01 - Desk"
    assert profile_rows["CW-P01"]["root_dir"].replace("\\", "/").endswith("/cw_provider/profiles/CW-P01")
    assert snapshots["ratings"]["actual"] == "20260101T000000Z-ratings.json"
    assert snapshots["_by_profile"]["CW-P01"]["ratings"]["actual"] == "20260202T000000Z-ratings.json"
    assert snapshots["_by_profile"]["CW-P01"]["ratings"]["provider_instance"] == "CW-P01"


def test_insights_snapshot_modal_is_crosswatch_profile_aware() -> None:
    insights_js = Path("assets/js/insights.js").read_text("utf-8")

    assert "CW_SNAPSHOT_PROFILE_KEY" in insights_js
    assert "_by_profile" in insights_js
    assert "cw-snap-profile-select" in insights_js
    assert "provider_instance=" in insights_js
    assert '"/config/.cw_provider/snapshots"' not in insights_js
