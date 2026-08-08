# CrossWatch test scripts
from __future__ import annotations

from typing import Any

import pytest

from providers.sync.simkl import _common, _history


@pytest.fixture(autouse=True)
def _reset() -> Any:
    _common.reset_user_settings_memo()
    _history._REWATCH_ACCOUNT_WARNED.clear()
    yield
    _common.reset_user_settings_memo()
    _history._REWATCH_ACCOUNT_WARNED.clear()


class _Resp:
    def __init__(self, payload: Any, status: int = 200) -> None:
        self._payload = payload
        self.status_code = status

    def json(self) -> Any:
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _Session:
    def __init__(self, payload: Any, status: int = 200) -> None:
        self.payload = payload
        self.status = status
        self.posts = 0

    def post(self, *_a: Any, **_k: Any) -> _Resp:
        self.posts += 1
        if isinstance(self.payload, Exception):
            raise self.payload
        return _Resp(self.payload, self.status)


class _Cfg:
    timeout = 10.0


class _Client:
    def __init__(self, session: _Session) -> None:
        self.session = session


class _Adapter:
    def __init__(self, session: _Session, *, rewatches: bool = True) -> None:
        self.client = _Client(session)
        self.cfg = _Cfg()
        self.config = {"_cw_history_rewatches": True} if rewatches else {}


def _adapter(tier: Any, *, status: int = 200, rewatches: bool = True) -> _Adapter:
    payload = tier if isinstance(tier, (Exception, dict)) else {"account": {"id": 1, "type": tier}}
    return _Adapter(_Session(payload, status), rewatches=rewatches)


@pytest.fixture(autouse=True)
def _stub_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_history, "adapter_headers", lambda a, **k: {"simkl-api-key": "k"})
    monkeypatch.setattr(_common, "simkl_api_params_from_headers", lambda h, **k: {})


# --- account type -------------------------------------------------------------


@pytest.mark.parametrize("tier,expected", [("pro", True), ("vip", True), ("free", False), ("", False)])
def test_rewatches_allowed_by_tier(tier: str, expected: bool) -> None:
    adapter = _adapter(tier)
    assert _history._rewatch_account_ok(adapter) is expected


def test_account_type_is_case_insensitive() -> None:
    assert _history._rewatch_account_ok(_adapter("VIP")) is True


# --- fail closed --------------------------------------------------------------


def test_unreachable_settings_disables_rewatches() -> None:
    assert _history._rewatch_account_ok(_adapter(RuntimeError("network down"))) is False


def test_error_status_disables_rewatches() -> None:
    assert _history._rewatch_account_ok(_adapter("pro", status=500)) is False


def test_malformed_payload_disables_rewatches() -> None:
    assert _history._rewatch_account_ok(_adapter({"nope": True})) is False


def test_missing_account_object_disables_rewatches() -> None:
    assert _history._rewatch_account_ok(_adapter({"user": {"name": "x"}})) is False


# --- interaction with the pair flag -------------------------------------------


def test_pair_flag_off_short_circuits_without_an_api_call() -> None:
    adapter = _adapter("pro", rewatches=False)
    assert _history._rewatches_enabled(adapter) is False
    assert adapter.client.session.posts == 0


def test_pair_flag_on_and_pro_enables() -> None:
    assert _history._rewatches_enabled(_adapter("pro")) is True


def test_pair_flag_on_and_free_disables() -> None:
    assert _history._rewatches_enabled(_adapter("free")) is False


# --- memoisation --------------------------------------------------------------


def test_settings_are_fetched_once_across_calls() -> None:
    adapter = _adapter("pro")
    for _ in range(4):
        assert _history._rewatches_enabled(adapter) is True
    assert adapter.client.session.posts == 1


def test_warning_is_not_repeated_per_call() -> None:
    adapter = _adapter("free")
    for _ in range(3):
        _history._rewatch_account_ok(adapter)
    assert _history._REWATCH_ACCOUNT_WARNED == {"free"}
