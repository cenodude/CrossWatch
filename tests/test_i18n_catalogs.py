# tests/test_i18n_catalogs.py
# CrossWatch - Translation catalog structure, coverage, fallback and language selection checks
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from cw_platform import i18n
from cw_platform.profile_preferences import clean_user_preferences

ROOT = Path(__file__).resolve().parent.parent
LOCALES = ROOT / "assets" / "locales"
PLACEHOLDER = re.compile(r"\{\{\s*(\w+)\s*\}\}")
RICH = re.compile(r"\[\[(/?)([a-z]+)\]\]")
CALL = re.compile(r"""\b(?:T|TH|TR)\(\s*["']([a-z][\w-]*(?:\.[\w-]+)+)["']""")
TOKEN = re.compile(r"\[\[t:([A-Za-z0-9_.-]+)")
DYNAMIC = re.compile(r"""\b(?:T|TH|TR)\(\s*`([a-z][\w.-]*)\$\{""")
PLURAL = re.compile(r"_(zero|one|two|few|many|other)$")


class _Request:
    def __init__(self, cookies: dict[str, str] | None = None) -> None:
        self.cookies = cookies or {}


def _codes() -> list[str]:
    return [row["code"] for row in i18n.registry()]


def _source_files() -> list[Path]:
    files = [path for path in (ROOT / "assets").rglob("*.js") if "locales" not in path.parts]
    return files + [ROOT / "ui_frontend.py", *sorted((ROOT / "api").glob("*.py"))]


def _known(messages: dict[str, str], key: str) -> bool:
    return key in messages or f"{key}_other" in messages


def test_registry_lists_english_first_and_dutch() -> None:
    rows = i18n.registry()
    assert rows[0] == {"code": "en", "name": "English"}
    assert {"code": "nl", "name": "Nederlands"} in rows
    assert i18n.DEFAULT_LANGUAGE == "en"


@pytest.mark.parametrize("code", ["en", "nl"])
def test_catalog_files_are_valid_json_without_duplicate_keys(code: str) -> None:
    files = sorted((LOCALES / code).glob("*.json"))
    assert [path.stem for path in files] == ["app", "common", "media", "settings"]
    for path in files:
        data = i18n.read_catalog_file(path)
        assert isinstance(data, dict) and data
        i18n.flatten(data)


def test_catalog_namespaces_do_not_overlap_between_files() -> None:
    for code in _codes():
        owners: dict[str, str] = {}
        for path in sorted((LOCALES / code).glob("*.json")):
            for namespace in json.loads(path.read_text(encoding="utf-8")):
                assert namespace not in owners, f"{code}: {namespace} in {owners.get(namespace)} and {path.name}"
                owners[namespace] = path.name


def test_every_language_has_the_same_keys_as_english() -> None:
    english = set(i18n.messages("en"))
    for code in _codes():
        assert set(i18n.messages(code)) == english, code


def test_placeholders_and_rich_markers_match_english() -> None:
    english = i18n.messages("en")
    for code in _codes():
        messages = i18n.messages(code)
        for key, text in english.items():
            other = messages[key]
            if not PLURAL.search(key) or key.endswith("_other"):
                assert set(PLACEHOLDER.findall(other)) == set(PLACEHOLDER.findall(text)), f"{code}:{key}"
            else:
                assert set(PLACEHOLDER.findall(other)) <= set(PLACEHOLDER.findall(text)), f"{code}:{key}"
            assert sorted(RICH.findall(other)) == sorted(RICH.findall(text)), f"{code}:{key}"


def test_plural_keys_come_in_complete_sets() -> None:
    for code in _codes():
        messages = i18n.messages(code)
        for key in messages:
            match = PLURAL.search(key)
            if match and match.group(1) == "one":
                assert key[: match.start()] + "_other" in messages, f"{code}:{key}"


def test_catalog_values_are_plain_text() -> None:
    for code in _codes():
        for key, text in i18n.messages(code).items():
            assert text.strip(), f"{code}:{key}"
            assert "<" not in RICH.sub("", text).replace("<-", "").replace("< ", ""), f"{code}:{key}"
            assert "&amp;" not in text and "&lt;" not in text, f"{code}:{key}"


def test_every_key_used_in_source_exists() -> None:
    english = i18n.messages("en")
    missing: dict[str, str] = {}
    for path in _source_files():
        text = path.read_text(encoding="utf-8", errors="replace")
        for key in [*CALL.findall(text), *TOKEN.findall(text)]:
            if not _known(english, key):
                missing.setdefault(key, path.name)
    assert not missing, missing


def test_dynamic_key_prefixes_resolve() -> None:
    english = i18n.messages("en")
    for path in _source_files():
        for prefix in DYNAMIC.findall(path.read_text(encoding="utf-8", errors="replace")):
            assert any(key.startswith(prefix) for key in english), f"{path.name}: {prefix}"


def test_lookup_falls_back_to_english_and_reports_missing_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    reports: list[str] = []
    monkeypatch.setattr(i18n, "_report", reports.append)
    nl = dict(i18n.messages("nl"))
    nl.pop("common.language.label")
    monkeypatch.setattr(i18n, "messages", lambda code: nl if i18n.clean_language(code) == "nl" else i18n._load_messages("en"))
    assert i18n.t("nl", "common.language.label") == "Language"
    assert i18n.t("nl", "no.such.key") == "no.such.key"
    assert reports == ["missing translation key no.such.key"]


def test_interpolation_plurals_and_escaping() -> None:
    assert i18n.t("en", "modals.support.items", count=1) == "1 item"
    assert i18n.t("en", "modals.support.items", count=3) == "3 items"
    assert i18n.t("nl", "modals.support.items", count=3) == "3 items"
    assert i18n.t("nl", "editor.parts.file", count=2) == "2 bestanden"
    assert i18n.h("en", "about.youAreOn", version='<b>"x"</b>').startswith("You are on v&lt;b&gt;&quot;x&quot;&lt;/b&gt;.")
    assert i18n.interpolate("{{a}} {{b}}", {"a": 1}) == "1 {{b}}"


def test_localize_html_resolves_tokens_with_arguments() -> None:
    page = "<p>[[t:common.language.label]]</p><p>[[t:modals.support.items|count=2]]</p>"
    assert i18n.localize_html(page, "en") == "<p>Language</p><p>2 items</p>"
    assert i18n.localize_html(page, "nl") == "<p>Taal</p><p>2 items</p>"


def test_clean_language_accepts_only_registered_codes() -> None:
    assert i18n.clean_language("NL") == "nl"
    assert i18n.clean_language(" nl ") == "nl"
    for value in (None, "", "de", "nl-BE", "../en", 5):
        assert i18n.clean_language(value) == "en"


def test_account_preference_wins_over_browser_cookie() -> None:
    request = _Request({i18n.LANGUAGE_COOKIE: "nl"})
    assert i18n.request_language(request, None) == "nl"
    assert i18n.request_language(request, {"preferences": {"language": "en"}}) == "en"
    assert i18n.request_language(_Request(), {"preferences": {"language": "nl"}}) == "nl"
    assert i18n.request_language(_Request(), {"preferences": {}}) == "en"
    assert i18n.request_language(None, None) == "en"


def test_language_preferences_are_kept_per_account() -> None:
    alice = {"preferences": clean_user_preferences({"language": "nl"})}
    bob = {"preferences": clean_user_preferences({})}
    assert alice["preferences"]["language"] == "nl"
    assert bob["preferences"]["language"] == "en"
    assert clean_user_preferences({"language": "xx"})["language"] == "en"
    assert i18n.user_language(alice) == "nl" and i18n.user_language(bob) == "en"


def test_browser_locale_never_selects_dutch() -> None:
    assert i18n.request_language(_Request({"Accept-Language": "nl-NL"}), None) == "en"


def test_bundle_and_head_tags_load_english_before_the_selected_language() -> None:
    bundle = i18n.bundle_js("nl")
    assert bundle.startswith('(window.__cwLocales=window.__cwLocales||{})["nl"]=')
    assert "</script" not in bundle
    head = i18n.head_tags("nl", "1.2.3", signed_in=True)
    assert head.index("/assets/locales/en.js") < head.index("/assets/locales/nl.js") < head.index("/assets/helpers/i18n.js")
    assert '"account":true' in head and "onerror=" in head
    assert "/assets/locales/nl.js" not in i18n.head_tags("en", "1")


def test_language_options_mark_the_current_choice() -> None:
    html = i18n.language_options_html("nl")
    assert '<option value="nl" lang="nl" selected>Nederlands</option>' in html
    assert '<option value="en" lang="en">English</option>' in html


def test_done_page_is_localized() -> None:
    page = i18n.done_page("nl", "auth.popup.plexComplete")
    assert '<html lang="nl">' in page
    assert i18n.h("nl", "auth.popup.done") in page


def test_broken_catalog_is_ignored_and_english_still_loads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for code in ("en", "nl"):
        (tmp_path / code).mkdir()
        for path in (LOCALES / code).glob("*.json"):
            (tmp_path / code / path.name).write_bytes(path.read_bytes())
    (tmp_path / "languages.json").write_bytes((LOCALES / "languages.json").read_bytes())
    (tmp_path / "nl" / "common.json").write_bytes(b"{ broken")
    reports: list[str] = []
    monkeypatch.setattr(i18n, "LOCALES_DIR", tmp_path)
    monkeypatch.setattr(i18n, "_report", reports.append)
    monkeypatch.setattr(i18n, "_CACHE", {}, raising=False)
    nl = i18n._load_messages("nl")
    assert "common.language.label" not in nl
    assert "about.youAreOn" in nl
    assert any("nl/common.json ignored" in line for line in reports)
