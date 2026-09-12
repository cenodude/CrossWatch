from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]


def test_shared_theme_has_no_priority_layer_or_surface_priority_rules():
    css = (ROOT / "assets/css/page-theme.css").read_text(encoding="utf-8")
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    assert "@layer" not in css
    for selector, body in re.findall(r"([^{}]+)\{([^{}]*)\}", css):
        if "!important" in body:
            assert "input" in selector or "focus-visible" in selector, selector
    # Temporary compatibility boundary for generic forms, not a growing theme
    # override layer. The boundary is documented next to the shared CSS.
    assert css.count("!important") <= 12


def test_legacy_page_headers_no_longer_own_surface_colours():
    for name in ("assets/css/pages.css", "assets/themes/flat.css"):
        css = (ROOT / name).read_text(encoding="utf-8")
        for selector, body in re.findall(r"([^{}]+)\{([^{}]*)\}", css):
            if re.search(r"\.cw-page-hero\s*$", selector):
                assert not re.search(r"(?:^|;)\s*(?:background(?:-color|-image)?|color|box-shadow)\s*:", body), selector


def test_shared_theme_is_in_both_document_shells():
    from ui_frontend import get_index_html, get_profile_html

    for html in (get_index_html(), get_profile_html()):
        assert html.count('href="/assets/css/page-theme.css?') == 1


def test_interactive_sync_empty_state_cannot_resize_other_pages():
    css = (ROOT / "assets/css/interactive-sync.css").read_text(encoding="utf-8")
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    selectors = [selector.strip() for selector, _ in re.findall(r"([^{}]+)\{([^{}]*)\}", css) if ".is-empty" in selector]
    assert selectors
    assert all(selector.startswith(".is-page .is-empty") for selector in selectors)


def test_flat_light_text_palette_meets_normal_text_contrast():
    css = (ROOT / "assets/themes/tokens.css").read_text(encoding="utf-8")
    values = {}
    for selector, body in re.findall(r"([^{}]+)\{([^{}]*)\}", css):
        if re.fullmatch(r'html\[data-cw-theme=(?:"flat-light"|flat-light)\]', selector.strip()):
            values.update(re.findall(r"(--[\w-]+):\s*([^;]+)", body))

    def luminance(value):
        value = values.get(value, value).strip()
        channels = [int(value[i:i + 2], 16) / 255 for i in (1, 3, 5)]
        linear = [c / 12.92 if c <= .04045 else ((c + .055) / 1.055) ** 2.4 for c in channels]
        return sum(c * weight for c, weight in zip(linear, (.2126, .7152, .0722)))

    for foreground, background in (
        ("--fg", "--panel"), ("--muted", "--panel2"),
        ("--accent", "--panel"), ("--accent2", "--panel2"),
        ("--danger", "--panel2"), ("--cw-theme-warning", "--panel2"),
        ("--cw-page-good-text", "--cw-page-good-bg"),
        ("--cw-page-danger-text", "--cw-page-danger-bg"),
        ("--cw-page-selected-text", "--cw-page-selected-bg"),
        ("#ffffff", "--cw-page-primary-bg"),
    ):
        light, dark = sorted((luminance(foreground), luminance(background)), reverse=True)
        assert (light + .05) / (dark + .05) >= 4.5, (foreground, background)


def test_capture_button_ids_do_not_raise_generic_button_specificity():
    css = (ROOT / "assets/themes/flat.css").read_text(encoding="utf-8")
    for group in re.findall(r":is\(([^()]*)\)", css):
        assert not (".btn.acc" in group and "#ss-create" in group)
