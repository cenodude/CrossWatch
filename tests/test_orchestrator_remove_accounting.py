from __future__ import annotations

from cw_platform.orchestrator._pairs_oneway import compute_effective_remove, is_remove_retry_reason


def _keys(n, prefix="imdb:tt"):
    return [f"{prefix}{i:04d}" for i in range(n)]


def test_exact_keys_select_only_confirmed_removals():
    attempted = _keys(87)
    confirmed = attempted[:25]

    out = compute_effective_remove(
        attempted_keys=attempted,
        provider_confirmed_count=87,
        provider_confirmed_keys=confirmed,
        provider_unresolved_count=62,
    )

    assert out["have_exact_keys"] is True
    assert out["ambiguous"] is False
    assert out["effective"] == 25
    assert out["success_keys"] == confirmed
    assert out["failed_keys"] == attempted[25:]
    assert len(out["failed_keys"]) == 62


def test_exact_keys_outside_attempted_set_are_ignored():
    out = compute_effective_remove(
        attempted_keys=["a", "b"],
        provider_confirmed_count=3,
        provider_confirmed_keys=["a", "zz"],
        provider_unresolved_count=0,
    )

    assert out["success_keys"] == ["a"]
    assert out["failed_keys"] == ["b"]
    assert out["effective"] == 1


def test_full_clean_count_without_exact_keys_is_accepted():
    attempted = _keys(10)

    out = compute_effective_remove(
        attempted_keys=attempted,
        provider_confirmed_count=10,
        provider_confirmed_keys=[],
        provider_unresolved_count=0,
        provider_errors=0,
    )

    assert out["have_exact_keys"] is False
    assert out["ambiguous"] is False
    assert out["success_keys"] == attempted
    assert out["failed_keys"] == []


def test_ambiguous_partial_without_exact_keys_confirms_nothing():
    attempted = _keys(10)

    out = compute_effective_remove(
        attempted_keys=attempted,
        provider_confirmed_count=4,
        provider_confirmed_keys=[],
        provider_unresolved_count=0,
    )

    assert out["ambiguous"] is True
    assert out["effective"] == 0
    assert out["success_keys"] == []
    assert out["failed_keys"] == attempted


def test_full_count_with_unresolved_or_errors_is_not_accepted():
    attempted = _keys(10)

    with_unresolved = compute_effective_remove(
        attempted_keys=attempted,
        provider_confirmed_count=10,
        provider_confirmed_keys=[],
        provider_unresolved_count=2,
    )
    with_errors = compute_effective_remove(
        attempted_keys=attempted,
        provider_confirmed_count=10,
        provider_confirmed_keys=[],
        provider_unresolved_count=0,
        provider_errors=1,
    )

    assert with_unresolved["success_keys"] == []
    assert with_errors["success_keys"] == []
    assert with_unresolved["failed_keys"] == attempted
    assert with_errors["failed_keys"] == attempted


def test_zero_confirmed_is_failure_not_ambiguous():
    attempted = _keys(5)

    out = compute_effective_remove(
        attempted_keys=attempted,
        provider_confirmed_count=0,
        provider_confirmed_keys=[],
        provider_unresolved_count=5,
    )

    assert out["ambiguous"] is False
    assert out["success_keys"] == []
    assert out["failed_keys"] == attempted


def test_remove_retry_reasons_are_selected_and_add_reasons_are_not():
    assert is_remove_retry_reason("apply:remove:unconfirmed") is True
    assert is_remove_retry_reason("two:apply:remove:unconfirmed") is True
    assert is_remove_retry_reason("provider_down:remove") is True

    assert is_remove_retry_reason("apply:add:failed") is False
    assert is_remove_retry_reason("two:apply:add:failed") is False
    assert is_remove_retry_reason("provider_down") is False
    assert is_remove_retry_reason("") is False
    assert is_remove_retry_reason(None) is False
