# tests/test_account_match.py
# CrossWatch - Media account matching tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from cw_platform.account_match import (
    exact_media_account_name,
    legacy_media_account_key,
    media_account_allowed,
    media_account_names_match,
    normalize_media_account_name,
)

WANG = chr(0x738B) + chr(0x660E)
LI = chr(0x674E) + chr(0x96F7)
E_ACUTE = chr(0xE9)
E_ACUTE_UPPER = chr(0xC9)
I_DIAERESIS = chr(0xEF)
COMBINING_ACUTE = chr(0x301)
COMBINING_SOLIDUS = chr(0x338)
JOSE = "Jos" + E_ACUTE
JOSE_DECOMPOSED = "Jose" + COMBINING_ACUTE
ELOISE = E_ACUTE_UPPER + "lo" + I_DIAERESIS + "se"
WIDE_ANNA = chr(0xFF21) + chr(0xFF4E) + chr(0xFF4E) + chr(0xFF41)
TRADEMARK = chr(0x2122)
WIDE_BANG = chr(0xFF01)
KA = chr(0x915)
KI = chr(0x915) + chr(0x93F)
CYRILLIC_ANNA = chr(0x430) + "nna"
GRIN = chr(0x1F600)
SMILE = chr(0x1F601)
NBSP = chr(0xA0)


def allowed(whitelist: list[str], account: str) -> bool:
    return media_account_allowed(whitelist, account, default_allow=False)


@pytest.mark.parametrize(
    ("value", "expected"),
    [("Dad", "dad"), ("  Anna B. ", "annab"), ("john_doe-99", "johndoe99"), ("", ""), (None, ""), ("!!!", "")],
)
def test_plain_names_normalise_as_before(value: object, expected: str) -> None:
    assert normalize_media_account_name(value) == expected


def test_plain_name_matching_is_unchanged() -> None:
    assert allowed(["dad"], "Dad") is True
    assert allowed(["Anna B."], "anna b") is True
    assert allowed(["john_doe"], "JohnDoe") is True
    assert allowed(["dad"], "kid") is False
    assert allowed(["dad"], "") is False


def test_plain_names_with_letters_or_digits_match_like_the_old_key() -> None:
    names = ["dad", "Dad", "D.A.D", "anna b", "Anna B.", "annab", "john_doe", "JohnDoe", "user99", "USER 99", "a", "b1", "x-y"]
    for entry in names:
        for account in names:
            old = legacy_media_account_key(entry) == legacy_media_account_key(account)
            assert allowed([entry], account) is old, (entry, account)


def test_non_latin_entry_does_not_match_an_empty_account() -> None:
    assert allowed([WANG], "") is False
    assert allowed([WANG], "   ") is False
    assert allowed([WANG], NBSP) is False
    assert allowed([WANG], "!!!") is False


def test_non_latin_entry_matches_only_its_own_name() -> None:
    assert allowed([WANG], WANG) is True
    assert allowed([WANG], f" {WANG} ") is True
    assert allowed([WANG], LI) is False
    assert allowed([WANG, LI], LI) is True


def test_accented_names_are_kept_apart_from_their_stripped_form() -> None:
    assert allowed([ELOISE], ELOISE.lower()) is True
    assert allowed([ELOISE], "lose") is False
    assert allowed([JOSE], "Jos") is False


def test_composed_and_decomposed_accents_are_the_same_name() -> None:
    assert JOSE != JOSE_DECOMPOSED
    assert allowed([JOSE], JOSE_DECOMPOSED) is True
    assert allowed([JOSE_DECOMPOSED], JOSE.upper()) is True


def test_compatibility_forms_are_not_folded_together() -> None:
    assert allowed([WIDE_ANNA], "anna") is False
    assert allowed(["anna"], WIDE_ANNA) is False
    assert allowed([WIDE_ANNA], WIDE_ANNA) is True
    assert allowed([TRADEMARK], "TM") is False
    assert allowed([TRADEMARK], TRADEMARK) is True
    assert allowed([WIDE_BANG], "!") is False


def test_marks_that_change_a_name_are_kept() -> None:
    assert allowed([KA], KI) is False
    assert allowed([KI], KA) is False
    assert allowed([KI], KI) is True
    assert allowed(["a"], "a" + COMBINING_SOLIDUS) is False


def test_look_alike_letters_from_another_script_do_not_match() -> None:
    assert allowed(["anna"], CYRILLIC_ANNA) is False


def test_entry_of_only_symbols_needs_the_same_symbols() -> None:
    assert allowed(["!!!"], "!!!") is True
    assert allowed(["!!!"], " !!! ") is True
    assert allowed(["!!!"], "???") is False
    assert allowed(["!!!"], "! ! !") is False
    assert allowed(["!!!"], "") is False
    assert allowed([GRIN], GRIN) is True
    assert allowed([GRIN], SMILE) is False


def test_empty_whitelist_entries_are_ignored() -> None:
    assert allowed(["", "  "], "") is False
    assert allowed(["", "dad"], "dad") is True


def test_id_and_uuid_selectors_still_work() -> None:
    assert media_account_allowed(["id:42"], "someone", account_id="42", default_allow=False) is True
    assert media_account_allowed(["id:42"], "someone", account_id="7", default_allow=False) is False
    assert media_account_allowed(["uuid:ABC"], "someone", account_uuid="abc", default_allow=False) is True
    assert media_account_allowed(["id:42"], "id:42", default_allow=False) is False


def test_names_match_helper() -> None:
    assert media_account_names_match("Anna B.", "annab") is True
    assert media_account_names_match("", "") is False
    assert media_account_names_match(WANG, "") is False
    assert media_account_names_match("", WANG) is False


def test_stored_keys_keep_the_old_form() -> None:
    assert legacy_media_account_key("Anna B.") == "annab"
    assert legacy_media_account_key(WANG) == ""
    assert legacy_media_account_key(None) == ""


def test_exact_name_tells_apart_what_loose_matching_merges() -> None:
    assert exact_media_account_name(" Anna B. ") == "anna b."
    assert exact_media_account_name("Anna B.") != exact_media_account_name("Annab")
    assert exact_media_account_name("!!!") != exact_media_account_name("")
    assert exact_media_account_name("!!!") != exact_media_account_name("???")
    assert exact_media_account_name(JOSE) == exact_media_account_name(JOSE_DECOMPOSED)


def test_plex_webhook_whitelist_uses_the_same_name_rules() -> None:
    from providers.webhooks.plex import _account_matches

    def payload(title: str, acc_id: str = "", uuid: str = "") -> dict:
        return {"Account": {"title": title, "id": acc_id, "uuid": uuid}}

    assert _account_matches({WANG}, payload(WANG)) is True
    assert _account_matches({WANG}, payload("")) is False
    assert _account_matches({WANG}, payload(LI)) is False
    assert _account_matches({"Anna B."}, payload("anna b")) is True
    assert _account_matches({"id:42"}, payload("x", acc_id="42")) is True
    assert _account_matches({"uuid:abc"}, payload("x", uuid="ABC")) is True
    assert _account_matches(set(), payload("")) is True


HEART = chr(0x2764) + chr(0xFE0F)
SUN = chr(0x2600) + chr(0xFE0F)


def test_different_emoji_with_a_variation_selector_do_not_match() -> None:
    assert normalize_media_account_name(HEART) == ""
    assert allowed([HEART], SUN) is False
    assert allowed([HEART], HEART) is True
    assert allowed([HEART], "") is False


def test_a_mark_only_counts_when_it_follows_a_letter_or_digit() -> None:
    assert normalize_media_account_name(COMBINING_ACUTE + "a") == "a"
    assert normalize_media_account_name("!" + COMBINING_ACUTE) == ""
    assert normalize_media_account_name(KI) == KI


ZERO_WIDTH_SPACE = chr(0x200B)
ZERO_WIDTH_JOINER = chr(0x200D)


def test_an_invisible_character_does_not_drop_the_mark_after_it() -> None:
    for hidden in (ZERO_WIDTH_SPACE, ZERO_WIDTH_JOINER):
        split = chr(0x915) + hidden + chr(0x93F)
        assert normalize_media_account_name(split) == KI
        assert allowed([KI], split) is True
        assert allowed([KA], split) is False
    assert allowed(["anna"], "an" + ZERO_WIDTH_SPACE + "na") is True
    assert normalize_media_account_name(ZERO_WIDTH_JOINER) == ""
    assert allowed([HEART], chr(0x2764) + ZERO_WIDTH_JOINER + chr(0xFE0F)) is False


def test_an_invisible_character_between_a_letter_and_its_accent_changes_nothing() -> None:
    for hidden in (ZERO_WIDTH_SPACE, ZERO_WIDTH_JOINER):
        split = "Jose" + hidden + COMBINING_ACUTE
        assert normalize_media_account_name(split) == normalize_media_account_name(JOSE)
        assert allowed([JOSE], split) is True
        assert allowed([split], JOSE_DECOMPOSED) is True
        assert allowed(["Jose"], split) is False
