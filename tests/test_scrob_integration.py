# CrossWatch test scripts
from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

ROOT = Path(__file__).resolve().parents[1]


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="ignore")


@pytest.fixture()
def isolated_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from cw_platform import config_base

    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(config_base, "CONFIG_JSON", tmp_path / "config.json", raising=False)
    return tmp_path


def test_registry_lists_scrob_auth_and_sync():
    from cw_platform.modules_registry import MODULES, get_sync_module_path_by_name

    assert MODULES["AUTH"]["_auth_SCROB"] == "providers.auth._auth_SCROB"
    assert MODULES["SYNC"]["_mod_SCROB"] == "providers.sync._mod_SCROB"
    assert get_sync_module_path_by_name("scrob") == "providers.sync._mod_SCROB"


def test_auth_runtime_resolves_the_scrob_backend():
    from providers.auth import runtime

    assert runtime.is_configured("scrob", {"server_url": "http://s", "api_key": "k", "username": "u", "password": "p"}) is True
    assert runtime.is_configured("scrob", {}) is False


def test_auth_registry_renders_scrob_in_the_tracker_group():
    from providers.auth.registry import auth_providers_html

    html = auth_providers_html()
    assert 'id="sec-scrob"' in html
    assert html.index('id="sec-scrob"') > html.index('id="sec-auth-trackers"')


def test_default_config_exposes_a_scrob_block(isolated_config: Path):
    from cw_platform.config_base import load_config

    cfg = load_config()
    block = cfg["scrob"]
    assert block["server_url"] == ""
    assert block["api_key"] == ""
    assert block["username"] == ""
    assert block["password"] == ""
    assert block["watchlist_name"] == "Watchlist"
    assert block["totp_enabled"] is False
    assert block["reauth_required"] is False
    assert "_pending" not in str(block)


def test_config_normalizer_cleans_the_scrob_block(isolated_config: Path):
    from cw_platform.config_base import load_config, save_config

    cfg = load_config()
    cfg["scrob"].update(
        {
            "server_url": "192.168.2.163:7330/",
            "api_prefix": "api/proxy/",
            "history_max_pages": 10**9,
            "expires_at": "not-a-number",
        }
    )
    save_config(cfg)
    fresh = load_config()["scrob"]
    assert fresh["server_url"] == "http://192.168.2.163:7330"
    assert fresh["api_prefix"] == "/api/proxy"
    assert fresh["history_max_pages"] == 100000
    assert fresh["expires_at"] == 0


def test_secrets_are_redacted(isolated_config: Path):
    from cw_platform.config_base import load_config, redact_config

    cfg = load_config()
    cfg["scrob"].update({"api_key": "KEY", "password": "PW", "access_token": "JWT", "username": "visible"})
    redacted = redact_config(cfg)
    for field in ("api_key", "password", "access_token"):
        assert redacted["scrob"][field] not in ("KEY", "PW", "JWT")
        assert redacted["scrob"][field]
    assert redacted["scrob"]["username"] == "visible"


def test_probe_key_hides_secrets_and_reports_unconfigured():
    from api.probesAPI import PROBE_CFG_KEY, PROVIDERS, _probe_key

    assert "scrob" in PROVIDERS
    assert PROBE_CFG_KEY["SCROB"] == "scrob"
    assert _probe_key("scrob", {"scrob": {}}) == "scrob|unconfigured"
    key = _probe_key("scrob", {"scrob": {"server_url": "http://host:7330", "api_key": "SECRET", "username": "bob"}})
    assert "SECRET" not in key
    assert "bob" not in key
    assert key.startswith("scrob|srv:")


def test_probe_and_userinfo_are_registered():
    from api.probesAPI import DETAIL_PROBES, USERINFO_FNS, _prov_configured

    assert "SCROB" in DETAIL_PROBES
    assert "SCROB" in USERINFO_FNS
    full = {"server_url": "http://s", "api_key": "k", "username": "u", "password": "p"}
    assert _prov_configured({"scrob": full}, "SCROB") is True
    assert _prov_configured({"scrob": {**full, "password": ""}}, "SCROB") is False
    assert _prov_configured({}, "SCROB") is False


def test_probe_reports_a_missing_connection_without_network():
    from api.probesAPI import _probe_scrob_detail

    ok, reason = _probe_scrob_detail({"scrob": {}})
    assert ok is False
    assert "Scrob" in reason


def test_scrob_is_a_route_provider_and_sink():
    from providers.scrobble.routes import ROUTE_PROVIDERS, ROUTE_RATING_SINKS, ROUTE_SINKS, normalize_route

    assert "scrob" in ROUTE_PROVIDERS
    assert "scrob" in ROUTE_SINKS
    assert "scrob" in ROUTE_RATING_SINKS
    route = normalize_route({"provider": "scrob", "sink": "trakt"}, "R1")
    assert route["provider"] == "scrob" and route["sink"] == "trakt"
    route = normalize_route({"provider": "plex", "sink": "scrob"}, "R2")
    assert route["sink"] == "scrob"


def test_watch_manager_builds_the_scrob_sink_and_watcher():
    from providers.scrobble.watch_manager import _make_sink, _make_watcher

    sink = _make_sink("scrob", lambda: {}, "default")
    assert sink.name == "scrob"
    watcher = _make_watcher("scrob", cast(Any, None), lambda: {}, "default")
    assert watcher.__class__.__name__ == "ScrobWatchService"


def test_webhook_dispatch_can_target_scrob():
    from providers.webhooks.config import _SINKS, sink_configured
    from providers.webhooks.dispatch import _make_sink

    assert "scrob" in _SINKS
    cfg = {"scrob": {"server_url": "http://s", "api_key": "k"}}
    assert sink_configured(cfg, "scrob") is True
    assert sink_configured({"scrob": {"server_url": "http://s"}}, "scrob") is False
    assert sink_configured({}, "scrob") is False
    assert _make_sink("scrob", "default", lambda: {}).name == "scrob"


def test_scrobbler_management_lists_scrob_as_a_watcher_source():
    from api.scrobblerManagementAPI import SINK_PROVIDERS, WATCHER_SOURCE_PROVIDERS

    assert "scrob" in WATCHER_SOURCE_PROVIDERS
    assert "scrob" in SINK_PROVIDERS


def test_scrobbler_management_marks_scrob_watcher_source_configured():
    from api.scrobblerManagementAPI import _scrobble_source_connected

    full = {"server_url": "http://s", "api_key": "k", "username": "u", "password": "p"}
    assert _scrobble_source_connected({"scrob": full}, "scrob", "default") is True
    assert _scrobble_source_connected({"scrob": {**full, "password": ""}}, "scrob", "default") is False


def test_playback_progress_registers_the_scrob_adapter():
    from services.playback_progress.service import PHASE1_PROVIDERS
    from services.playback_progress.adapters.scrob import ScrobPlaybackAdapter

    assert "scrob" in PHASE1_PROVIDERS
    adapter = ScrobPlaybackAdapter()
    caps = adapter.capabilities({}, instance_id="default", instance_label="Scrob")
    assert caps.configured is False
    assert caps.read is False
    assert "not connected" in caps.reason

    configured = {"scrob": {"server_url": "http://s", "api_key": "k", "username": "u", "password": "p"}}
    caps = adapter.capabilities(configured, instance_id="default", instance_label="Scrob")
    assert caps.configured is True
    assert caps.read is True
    assert caps.supports_movies is True and caps.supports_episodes is True


def test_history_event_fields_are_preserved_end_to_end():
    from cw_platform.history_events import EVENT_ID_FIELDS, history_event_key
    from cw_platform.id_map import minimal as id_minimal

    assert "_scrob_history_id" in EVENT_ID_FIELDS
    item = {
        "type": "movie",
        "ids": {"tmdb": "550"},
        "watched_at": "2026-08-01T20:00:00.000Z",
        "_scrob_history_id": "11",
        "_scrob_media_id": "5",
    }
    mini = id_minimal(item)
    assert mini["_scrob_history_id"] == "11"
    assert mini["_scrob_media_id"] == "5"
    assert history_event_key(mini).endswith("@1785621600") or "@" in history_event_key(mini)


def test_branding_assets_and_frontend_wiring():
    logo = ROOT / "assets/img/SCROB.png"
    assert logo.exists()
    assert 6_000 <= logo.stat().st_size <= 32_000

    meta = read("assets/helpers/provider-meta.js")
    assert 'SCROB: { key: "SCROB"' in meta
    assert 'logoFile: "SCROB.png"' in meta
    assert 'rgb: "111,54,204"' in meta
    assert '"SCROB"' in meta.split("const order")[1].split("]")[0]

    providers_css = read("assets/css/providers.css")
    assert ".prov-card.brand-scrob" in providers_css
    assert "/assets/img/SCROB.png" in providers_css
    assert "--scrob-rgb:111,54,204" in providers_css
    assert "#sec-scrob>.head" in providers_css
    assert '#stats-card #stat-providers .tile[data-provider="scrob"]' in providers_css
    assert '#page-snapshots .ss-badge[data-provider="scrob"]' in providers_css
    assert '#details .det-structured-line[data-provider="SCROB"]' in providers_css
    assert ".wl-provider-card.provider-scrob" in providers_css
    assert providers_css.count("{") == providers_css.count("}")

    assert "#scrob_disconnect" in read("assets/css/auth-providers.css")
    assert "#scrob_connect" in read("assets/themes/flat.css")

    loader = read("assets/auth/auth_loader.js")
    assert 'scrob: "/assets/auth/auth.scrob.js"' in loader
    assert '"sec-scrob": "scrob"' in loader

    core = read("assets/helpers/core.js")
    assert "hasScrobConfig" in core
    assert 'set.add("SCROB")' in core

    assert '"SCROB"' in read("assets/helpers/providers-ui.js")
    assert 'scrob: "crosswatch/settings/connections/trackers/scrob"' in read("assets/helpers/help-links.js")
    assert 'scrob: "Scrob"' in read("assets/js/modals/scrobbler-route/index.js")


def test_auth_section_html_exposes_every_field():
    from providers.auth._auth_SCROB import html

    markup = html()
    for element_id in ("scrob_server", "scrob_key", "scrob_username", "scrob_password", "scrob_totp", "scrob_verify_ssl", "scrob_connect", "scrob_disconnect", "scrob_msg"):
        assert f'id="{element_id}"' in markup
    assert "/assets/img/SCROB.png" in markup


def test_scrob_connection_modal_stages_auth_until_settings_save():
    ui = read("assets/auth/auth.scrob.js")
    save = read("assets/helpers/settings-save.js")
    api = read("api/authenticationAPI.py")

    assert '/api/scrob/save?validate_only=1' in ui
    assert "Scrob connected - save settings to apply" in ui
    assert "window.__cwScrobPendingAuth" in ui
    assert "_cwValidateScrobSecret" in save
    assert "/api/scrob/save?instance=${encodeURIComponent(_cwNormInst(inst))}&validate_only=1" in save
    assert "validate_only: bool = Query(False)" in api
    assert "if validate_only:" in api


def test_auth_manifest_marks_required_fields():
    from providers.auth._auth_SCROB import PROVIDER

    manifest = PROVIDER.manifest()
    required = {f["key"] for f in manifest.fields if f.get("required")}
    assert required == {"scrob.server_url", "scrob.api_key", "scrob.username", "scrob.password"}
    assert {f["key"] for f in manifest.fields if not f.get("required")} == {"scrob.verify_ssl", "scrob.totp_code"}
    assert manifest.actions["start"] is False
    assert PROVIDER.capabilities() == {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False}


def test_new_files_carry_the_standard_header():
    for rel in (
        "providers/auth/_auth_SCROB.py",
        "providers/sync/_mod_SCROB.py",
        "providers/sync/scrob/__init__.py",
        "providers/sync/scrob/_common.py",
        "providers/sync/scrob/_history.py",
        "providers/sync/scrob/_ratings.py",
        "providers/sync/scrob/_progress.py",
        "providers/sync/scrob/_watchlist.py",
        "providers/scrobble/scrob/__init__.py",
        "providers/scrobble/scrob/sink.py",
        "providers/scrobble/scrob/watch.py",
        "services/playback_progress/adapters/scrob.py",
        "assets/auth/auth.scrob.js",
    ):
        head = read(rel).splitlines()[:3]
        assert any("CrossWatch" in line for line in head), rel
        assert any("Copyright" in line for line in head), rel


def test_pair_config_modal_can_rename_the_scrob_list():
    js = read("assets/js/modals/pair-config/index.js")

    assert 'const isScrob=(v)=>same(v,"scrob");' in js
    assert "function hasScrob(state){return isScrob(state?.src)||isScrob(state?.dst)}" in js

    assert '"#cx-scrob-wl-name",' in js
    assert 'state.cfgRaw?.scrob?.watchlist_name || "Watchlist"' in js
    assert '<label for="cx-scrob-wl-name">List name</label>' in js
    assert 'id="cx-scrob-wl-name"' in js
    assert "${hasScrob(state)?`" in js

    assert 'if(id==="cx-scrob-wl-name"){' in js
    assert "state.pairProviders.scrob=sb;" in js

    assert 'const useScrob=(String(src).toUpperCase()==="SCROB"||String(dst).toUpperCase()==="SCROB");' in js
    assert "prov.scrob=Object.assign({},sbSrc,{watchlist_name:name});" in js

    scrob_field = js[js.index('${hasScrob(state)?`'):]
    assert "Scrob specifics" in scrob_field[:600]


def test_pair_config_keeps_unknown_provider_keys():
    from api.syncAPI import _normalize_pair_providers

    out = _normalize_pair_providers({"scrob": {"watchlist_name": "Plan to watch"}})
    assert out == {"scrob": {"watchlist_name": "Plan to watch"}}


def test_connection_modal_puts_the_journey_and_steps_above_the_form():
    import re

    js = read("assets/helpers/providers-ui.js")
    block = js[js.index("    SCROB: {"):js.index("    PUNCHPLAY: {")]

    order = re.search(r"order: \[(.*?)\]", block)
    assert order, "SCROB needs an order array or every field defaults to css order 0 and floats above the journey"

    selectors = [x.strip().strip('"').strip("'") for x in order.group(1).split(",") if x.strip()]
    assert selectors[0] == ".grid2"
    for element in ("#scrob_totp_row", "#scrob_reauth", ".verify", "#scrob_actions_row"):
        assert element in selectors, element

    markup = read("providers/auth/_auth_SCROB.py")
    for selector in selectors:
        token = selector.lstrip("#.")
        assert token in markup, f"{selector} is ordered but no element matches it in html()"


def test_every_connection_modal_provider_declares_an_order():
    import re

    js = read("assets/helpers/providers-ui.js")
    missing = []
    for match in re.finditer(r"\n    ([A-Z_]+): \{\n(.*?)\n    \},", js, re.S):
        name, body = match.group(1), match.group(2)
        if "journey:" in body and "order:" not in body:
            missing.append(name)
    assert not missing, f"these providers render their form above the journey banner: {missing}"


def test_connection_modal_promotes_the_status_into_the_shared_pill():
    js = read("assets/helpers/providers-ui.js")
    block = js[js.index("    SCROB: {"):js.index("    PUNCHPLAY: {")]

    assert 'actions: [{ row: "#scrob_actions_row", status: "#scrob_msg", buttons: "#scrob_connect" }]' in block, (
        "without an actions entry #scrob_msg never becomes .cw-connection-status-pill, so the modal "
        "cannot tell it is connected: Save stays disabled and the success burst never fires"
    )
    assert 'tabs: {' in block

    markup = read("providers/auth/_auth_SCROB.py")
    for element in ("scrob_actions_row", "scrob_msg", "scrob_connect"):
        assert f'id="{element}"' in markup, element


def test_every_profile_provider_declares_a_status_action():
    import re

    js = read("assets/helpers/providers-ui.js")

    hardcoded = re.search(r'querySelectorAll\("(#plex_msg[^"]*)"\)', js)
    assert hardcoded, "connectionModalStatusTarget no longer has a hardcoded selector list"
    builtin = {sel.strip().lstrip("#").replace("_msg", "") for sel in hardcoded.group(1).split(",") if "_msg" in sel}
    assert builtin == {"plex", "jfy", "emby"}
    exempt = {"PLEX", "JELLYFIN", "EMBY", "TMDB_METADATA", "ANIME_MAPPING"}

    missing = []
    for match in re.finditer(r"\n    ([A-Z_]+): \{\n(.*?)\n    \},", js, re.S):
        name, body = match.group(1), match.group(2)
        if name in exempt or "journey:" not in body:
            continue
        if "status:" not in body:
            missing.append(name)
    assert not missing, f"these providers cannot report connected state to the modal: {missing}"

def test_scrob_profile_detection_requires_the_full_credential_set():
    js = read("assets/helpers/providers-ui.js")
    line = next(l for l in js.splitlines() if 'if (p === "scrob")' in l)
    for field in ("server_url", "api_key", "username", "password"):
        assert field in line, f"{field} missing from the scrob profile check"
    assert "||" not in line.split("return", 1)[1], "the scrob check must require every field, not any of them"


def test_scrob_logo_is_not_cropped_to_its_empty_centre():
    import re

    pages = read("assets/css/pages.css")
    shell = read("assets/crosswatch.css")
    providers = read("assets/css/providers.css")

    tile_base = re.search(r"#stats-card #stat-providers \.tile::after\{([^}]*)\}", pages).group(1)
    assert "width:220%" in tile_base
    tile = re.search(r'#stats-card #stat-providers \.tile\[data-provider="scrob"\]::after\{([^}]*)\}', providers)
    assert tile, "the statistics tile has no Scrob zoom override"
    assert int(re.search(r"width:(\d+)%", tile.group(1)).group(1)) <= 120

    card = re.search(r'#providers_list \.prov-card\.brand-scrob \.prov-watermark::after\{([^}]*)\}', providers)
    assert card, "the provider list card has no Scrob zoom override"
    assert int(re.search(r"width:(\d+)%", card.group(1)).group(1)) <= 120

    pill_rules = re.findall(r"#conn-badges \.conn-provider-visual\{([^}]*)\}", shell)
    assert pill_rules, "the connection pill visual rule moved"
    assert "220% 220%" in pill_rules[-1], "the pill no longer zooms to 220%, revisit the Scrob override"
    assert "content:none" in re.findall(r"#conn-badges \.conn-provider-visual::after\{([^}]*)\}", shell)[-1], (
        "the pill ::after is live again, so a ::after based override would be the right fix"
    )

    pill = re.search(
        r'#conn-badges \.conn-pill\[data-prov="SCROB"\] \.conn-provider-visual\{([^}]*)\}', providers
    )
    assert pill, "the connection pill has no Scrob override, so the logo renders as its empty middle"
    assert "background-size" in pill.group(1) and "!important" in pill.group(1), (
        "the base rule uses !important and an id selector, so the override must too"
    )
    assert "220%" not in pill.group(1)

    assert providers.count("{") == providers.count("}")


def test_scrob_connection_panel_is_compacted_to_fit_without_scrolling():
    import re

    css = read("assets/css/auth-providers.css")
    scoped = f'#page-settings #auth-providers .cw-connection-modal-panel[data-cw-connection-provider="scrob"]'

    before = re.search(re.escape(scoped) + r"::before\{([^}]*)\}", css)
    assert before, "the Scrob panel watermark is not limited"
    base = re.search(r"#page-settings #auth-providers \.cw-connection-modal-panel::before\{([^}]*)\}", css).group(1)
    assert "430px" in base and "430px" not in before.group(1)

    journey = re.search(re.escape(scoped) + r" \.cw-auth-journey\{([^}]*)\}", css)
    assert journey, "the Scrob journey banner is not compacted"
    assert "!important" in journey.group(1), "the base journey margin uses !important"

    markup = read("providers/auth/_auth_SCROB.py")
    assert 'style="margin-top:12px"' not in markup

    assert css.count("{") == css.count("}")
