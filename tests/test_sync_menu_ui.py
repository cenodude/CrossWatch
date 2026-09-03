from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_sync_pair_menu_trigger_toggles_from_child_clicks() -> None:
    core = (ROOT / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")

    assert "const liveBtn = byId(\"run-menu\");" in core
    assert "liveBtn?.contains?.(target)" in core
    assert "if (!menu.classList.contains(\"hidden\"))" in core
    assert "cwCloseSyncMenu();" in core


def test_sync_pair_menu_shows_enabled_feature_dots() -> None:
    core = (ROOT / "assets" / "helpers" / "core.js").read_text(encoding="utf-8")
    css = (ROOT / "assets" / "crosswatch.css").read_text(encoding="utf-8")

    assert "function enabledPairFeatures(pair)" in core
    assert "function syncFeatureDotsHTML(pair)" in core
    assert "class=\"cw-sync-menu-features\" role=\"img\"" in core
    assert "class=\"cw-sync-feature-dot ${cls}\"" in core
    assert "aria-hidden=\"true\"" in core
    assert ".cw-sync-menu-main{" in css
    assert ".cw-sync-feature-dot.wl{--dot-rgb:0,255,163}" in css
    assert ".cw-sync-feature-dot.rt{--dot-rgb:255,196,0}" in css
    assert ".cw-sync-feature-dot.hi{--dot-rgb:45,226,255}" in css
    assert ".cw-sync-feature-dot.pr{--dot-rgb:167,139,250}" in css
    assert ".cw-sync-feature-dot.pl{--dot-rgb:255,0,229}" in css
    assert ".cw-sync-feature-dot.co{--dot-rgb:255,122,26}" in css


def test_sync_pair_menu_feature_dots_respect_flat_theme() -> None:
    flat = (ROOT / "assets" / "themes" / "flat.css").read_text(encoding="utf-8")

    assert "html[data-cw-theme] #cw-sync-menu .cw-sync-feature-dot{box-shadow:none !important}" in flat
