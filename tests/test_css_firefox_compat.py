from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _asset_css_files() -> list[Path]:
    return sorted((ROOT / "assets").rglob("*.css"))


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_asset_css_blocks_are_balanced() -> None:
    problems = []
    for path in _asset_css_files():
        css = path.read_text(encoding="utf-8")
        if css.count("{") != css.count("}"):
            problems.append(path.relative_to(ROOT).as_posix())

    assert not problems


def test_webkit_text_fill_keeps_standard_color_fallbacks() -> None:
    missing = []
    for path in _asset_css_files():
        css = path.read_text(encoding="utf-8")
        offset = 0
        while True:
            offset = css.find("-webkit-text-fill-color", offset)
            if offset == -1:
                break
            start = css.rfind("{", 0, offset)
            end = css.find("}", offset)
            block = css[start + 1 : end if end != -1 else len(css)]
            if "color:" not in block:
                line = css.count("\n", 0, offset) + 1
                missing.append(f"{path.relative_to(ROOT).as_posix()}:{line}")
            offset += len("-webkit-text-fill-color")

    assert not missing


def test_editor_layout_has_firefox_safe_wrap_points() -> None:
    css = _read("assets/css/pages.css")

    assert "#page-editor .cw-root,#page-editor .cw-wrap,#page-editor .cw-main,#page-editor .cw-side,#page-editor .cw-controls,#page-editor .cw-topline.cw-page-hero{box-sizing:border-box;max-width:100%;min-width:0}" in css
    assert "#page-editor .cw-controls{flex-wrap:wrap !important}" in css
    assert "#page-editor .cw-topline.cw-page-hero{grid-template-columns:minmax(0,1fr)minmax(0,auto) !important}" in css
    assert "@media(max-width:1560px){#page-editor .cw-editor-hero-summary{justify-self:end;flex-wrap:wrap}" in css
    assert "@media(max-width:1560px){#page-editor .cw-topline.cw-page-hero{grid-template-columns:1fr !important}" not in css
    assert "@media(max-width:1320px){#page-editor .cw-wrap{grid-template-columns:minmax(0,1fr)}" in css
    assert "#page-editor .cw-side{display:none}}" in css


def test_editor_empty_state_keeps_icon_centered_in_firefox() -> None:
    css = _read("assets/css/pages.css")
    table_controller_js = _read("assets/js/editor/table-controller.js")

    assert 'ctx.empty.style.display = "grid"' in table_controller_js
    assert 'ctx.empty.style.display = "block"' not in table_controller_js
    assert ".cw-main.cw-main-empty .cw-empty{flex:0 0 auto;display:grid;grid-template-columns:minmax(0,1fr);justify-items:center;align-content:center;" in css
    assert ".cw-empty .cw-empty-icon{width:54px;height:54px;display:grid;place-items:center;justify-self:center;align-self:center;margin:0 auto;float:none;position:static;" in css


def test_firefox_scrollbar_fallbacks_cover_key_scroll_regions() -> None:
    base_css = _read("assets/css/base.css")
    pages_css = _read("assets/css/pages.css")

    assert "html{scrollbar-width:thin;scrollbar-color:var(--cw-scrollbar-thumb)var(--cw-scrollbar-track)}" in base_css
    assert ".cw-table-scroll{scrollbar-width:thin;scrollbar-color:#8b5cf6 #10131a}" in pages_css
    assert ".cw-editor-send-card .body{min-height:0;overflow-x:hidden;overflow-y:auto;overscroll-behavior:contain;scrollbar-gutter:stable;scrollbar-width:thin;scrollbar-color:var(--cw-scrollbar-thumb,#8b5cf6)var(--cw-scrollbar-track,#10131a)}" in pages_css


def test_disabled_and_locked_controls_use_block_cursor() -> None:
    crosswatch_css = _read("assets/crosswatch.css")
    pages_css = _read("assets/css/pages.css")

    assert ".btn:disabled,.iconbtn:disabled{opacity:0.55;cursor:not-allowed;box-shadow:none}" in crosswatch_css
    assert "#page-snapshots .ss-capture-running .ss-item{cursor:not-allowed}" in pages_css
    assert "#page-watchlist #wl-filter-state:disabled{opacity:.52;cursor:not-allowed}" in pages_css
