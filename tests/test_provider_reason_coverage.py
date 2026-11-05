# tests/test_provider_reason_coverage.py
# CrossWatch - User-Facing Provider Reason Coverage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from cw_platform.reason_labels import friendly_reason, reason_message
from cw_platform.reason_catalog import ALIASES, OUTCOMES


ROOT = Path(__file__).resolve().parents[1]
HELPERS = {"unresolved_entry", "_unresolved_entry", "unresolved_item", "_unresolved_item", "unresolved", "_unresolved", "_unres", "_unresolved_for_items"}
RESULT_STATUSES = {"failed", "unresolved", "skipped", "applied", "updated", "removed", "added", "dry_run", "ok", "error", "success"}


def literals(value):
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        yield value.value
    elif isinstance(value, (ast.BoolOp, ast.IfExp)):
        for child in ast.iter_child_nodes(value):
            yield from literals(child)


def returned_reasons(tree):
    for node in ast.walk(tree):
        roots = []
        if isinstance(node, ast.Call):
            name = ast.unparse(node.func)
            if name.endswith((".append", ".extend")) and any(part in name.lower() for part in ("unresolved", "failed", "failures")):
                roots = node.args
            elif name.split(".")[-1] in HELPERS | {"_fail"}:
                if name.split(".")[-1] in HELPERS:
                    for value in node.args[1:2]:
                        for code in literals(value):
                            yield node.lineno, code
                roots = [node]
        elif isinstance(node, ast.Assign) and any("unresolved" in ast.unparse(target).lower() for target in node.targets):
            roots = [node.value]
        elif isinstance(node, ast.Dict) and any(isinstance(k, ast.Constant) and k.value == "status"
                and isinstance(v, ast.Constant) and v.value in {"failed", "unresolved", "skipped"}
                for k, v in zip(node.keys, node.values)):
            roots = [node]
        elif isinstance(node, ast.Return) and isinstance(node.value, ast.Tuple) and len(node.value.elts) >= 2:
            first = node.value.elts[0]
            if isinstance(first, ast.Constant) and first.value is None:
                for value in node.value.elts[1:]:
                    for code in literals(value):
                        yield node.lineno, code
        for root in roots:
            for child in ast.walk(root):
                if isinstance(child, ast.Dict):
                    for key, value in zip(child.keys, child.values):
                        if isinstance(key, ast.Constant) and key.value in {"reason", "hint", "status"}:
                            for code in literals(value):
                                if key.value != "status" or code not in RESULT_STATUSES:
                                    yield child.lineno, code
                elif isinstance(child, ast.keyword) and child.arg in {"reason", "hint"}:
                    for code in literals(child.value):
                        yield child.lineno, code


def emitted_cases():
    cases = set()
    for path in sorted((ROOT / "providers" / "sync").rglob("*.py")):
        relative = path.relative_to(ROOT)
        provider = relative.parts[2].upper() if len(relative.parts) > 3 else path.stem.removeprefix("_mod_").upper()
        feature = path.stem.lstrip("_")
        for line, code in returned_reasons(ast.parse(path.read_text(encoding="utf-8-sig"))):
            if code:
                cases.add((provider, feature, code, f"{relative.as_posix()}:{line}"))
    return sorted(cases)


@pytest.mark.parametrize("provider,feature,code,source", emitted_cases())
def test_literal_item_outcomes_have_messages(provider, feature, code, source):
    assert reason_message(code, provider=provider, feature=feature), f"{source}: {code}"
    label = friendly_reason(code)
    assert label != code and len(label) <= 80, f"{source}: short label missing for {code}"


def test_scan_excludes_logging_but_keeps_returned_failure():
    tree = ast.parse('''
_dbg("write_prepare", reason="capacity_partial")
_info("custom_lists_skipped", reason="account_tier")
unresolved.append({"item": item, "hint": "trakt_limit"})
''')
    assert list(returned_reasons(tree)) == [(4, "trakt_limit")]


def test_scan_catches_status_only_failures_without_classification_labels():
    tree = ast.parse('''
unresolved.append({"key": key, "status": "outside_supported_progress_window"})
results.append({"status": "skipped", "reason": "active_session"})
''')
    assert list(returned_reasons(tree)) == [(2, "outside_supported_progress_window"), (3, "active_session")]


@pytest.mark.parametrize("code", ["activities_unchanged", "baseline_fetch", "capacity_partial", "account_tier", "key_collision", "snapshot_not_found", "future_unknown_reason"])
def test_debug_and_unknown_codes_are_not_guessed(code):
    assert reason_message(code) == ""


def test_skips_and_unconfirmed_writes_are_not_reported_as_failed_requests():
    assert "preserved" in reason_message("existing_newer").lower()
    assert "No update was needed" in reason_message("stremio_rating_already_set")
    assert "may already have taken effect" in reason_message("write_not_verified")
    assert "Check" in reason_message("trakt_limit")
    assert "Select and apply" not in reason_message("write_not_verified")


@pytest.mark.parametrize("code", [
    "invalid_api_path", "invalid_json", "request_failed", "request_rejected", "invalid_api_token",
    "missing_media_identity", "missing_episode_show", "missing_supported_id", "invalid_episode_coordinates",
    "unsupported_media_type", "existing_watch_status_preserved", "rating_must_be_1_to_10",
    "anime_media_type_mismatch", "episode_out_of_range", "not_anime_or_no_mapping",
    "kitsu_history_unknown_completed_total", "myanimelist_history_unknown_completed_total",
    "add_failed:ValueError", "remove_failed:RuntimeError", "write_failed:TimeoutError",
    "http:401", "http_403", "http:420", "http_426", "http:429", "http_503", "http:418",
    "floppy_http_401:private response", "floppy_http_503:private response", "floppy_error:private response",
    "active_session", "same_origin", "target_newer",
])
def test_indirect_and_dynamic_outcomes_have_safe_messages(code):
    assert reason_message(code)
    assert friendly_reason(code) != code
    assert "private response" not in reason_message(code)
    assert "private response" not in friendly_reason(code)


def test_catalog_aliases_and_short_labels_are_valid():
    assert set(ALIASES.values()) <= OUTCOMES.keys()
    for label, message in OUTCOMES.values():
        assert label and len(label) <= 80
        assert message.endswith(".")


def test_unknown_exception_details_do_not_gain_an_invented_message():
    assert reason_message("write_failed:arbitrary private response") == ""
    assert reason_message("future_provider_write_failed") == ""
