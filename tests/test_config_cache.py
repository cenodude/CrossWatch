from __future__ import annotations

import json
import os
import threading
from pathlib import Path

import pytest

from cw_platform import config_base as cb


@pytest.fixture(autouse=True)
def _clean_caches():
    cb.invalidate_config_cache()
    with cb._KEY_CACHE_LOCK:
        cb._KEY_CACHE.clear()
        cb._CIPHER_CACHE.clear()
    yield
    cb.invalidate_config_cache()


def _write_raw(base: Path, payload: dict) -> None:
    (base / "config.json").write_text(json.dumps(payload), encoding="utf-8")


def test_token_refresh_is_visible_to_the_next_load(config_base: Path):
    cfg = cb.load_config()
    cfg.setdefault("trakt", {})["access_token"] = "token-a"
    cb.save_config(cfg)
    assert cb.load_config()["trakt"]["access_token"] == "token-a"

    refreshed = cb.load_config()
    refreshed["trakt"]["access_token"] = "token-b"
    refreshed["trakt"]["refresh_token"] = "refresh-b"
    cb.save_config(refreshed)

    again = cb.load_config()
    assert again["trakt"]["access_token"] == "token-b"
    assert again["trakt"]["refresh_token"] == "refresh-b"


def test_tokens_are_stored_encrypted_and_read_back(config_base: Path):
    cfg = cb.load_config()
    cfg.setdefault("trakt", {})["access_token"] = "secret-token"
    cb.save_config(cfg)

    on_disk = json.loads((config_base / "config.json").read_text("utf-8"))
    assert on_disk["trakt"]["access_token"].startswith("enc:v1:")
    assert cb.load_config()["trakt"]["access_token"] == "secret-token"


def test_repeated_loads_do_not_reread_the_master_key(config_base: Path):
    cfg = cb.load_config()
    cfg.setdefault("trakt", {})["access_token"] = "token-a"
    cb.save_config(cfg)
    cb.load_config()

    reads = {"n": 0}
    real = Path.read_text

    def counting(self, *a, **k):
        if self.name == ".cw_master_key":
            reads["n"] += 1
        return real(self, *a, **k)

    Path.read_text = counting  # type: ignore[method-assign]
    try:
        for _ in range(20):
            assert cb.load_config()["trakt"]["access_token"] == "token-a"
    finally:
        Path.read_text = real  # type: ignore[method-assign]
    assert reads["n"] == 0


def test_an_external_edit_is_picked_up(config_base: Path):
    _write_raw(config_base, {"trakt": {"client_id": "one"}})
    assert cb.load_config()["trakt"]["client_id"] == "one"

    _write_raw(config_base, {"trakt": {"client_id": "two-longer-value"}})
    assert cb.load_config()["trakt"]["client_id"] == "two-longer-value"


def test_a_same_size_external_edit_is_picked_up(config_base: Path):
    path = config_base / "config.json"
    _write_raw(config_base, {"trakt": {"client_id": "aaa"}})
    assert cb.load_config()["trakt"]["client_id"] == "aaa"

    stat = path.stat()
    _write_raw(config_base, {"trakt": {"client_id": "bbb"}})
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
    assert path.stat().st_size == stat.st_size
    assert cb.load_config()["trakt"]["client_id"] == "bbb"


def test_a_save_invalidates_even_when_the_file_stamp_cannot_tell(config_base: Path, monkeypatch):
    monkeypatch.setattr(cb, "_config_stamp", lambda p: ("fixed",))

    cfg = cb.load_config()
    cfg.setdefault("trakt", {})["client_id"] = "id-a"
    cb.save_config(cfg)
    assert cb.load_config()["trakt"]["client_id"] == "id-a"

    nxt = cb.load_config()
    nxt["trakt"]["client_id"] = "id-b"
    cb.save_config(nxt)
    assert cb.load_config()["trakt"]["client_id"] == "id-b"


def test_callers_cannot_mutate_the_cached_config(config_base: Path):
    cfg = cb.load_config()
    cfg.setdefault("trakt", {})["access_token"] = "token-a"
    cb.save_config(cfg)

    first = cb.load_config()
    first["trakt"]["access_token"] = "clobbered"
    first["pairs"] = [{"id": "injected"}]

    second = cb.load_config()
    assert second["trakt"]["access_token"] == "token-a"
    assert second.get("pairs") == []


def test_concurrent_loads_see_a_saved_token(config_base: Path):
    cfg = cb.load_config()
    cfg.setdefault("trakt", {})["access_token"] = "token-a"
    cb.save_config(cfg)

    seen: list[str] = []
    errors: list[BaseException] = []
    stop = threading.Event()

    def reader():
        try:
            while not stop.is_set():
                seen.append(str(cb.load_config().get("trakt", {}).get("access_token")))
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    try:
        for i in range(5):
            nxt = cb.load_config()
            nxt["trakt"]["access_token"] = f"token-{i}"
            cb.save_config(nxt)
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=10)

    assert not errors
    assert seen
    assert set(seen) <= {"token-a", "token-0", "token-1", "token-2", "token-3", "token-4"}
    assert cb.load_config()["trakt"]["access_token"] == "token-4"


class _FakeTokenResponse:
    status_code = 200

    def json(self):
        return {"access_token": "fresh-access", "refresh_token": "fresh-refresh", "expires_in": 7776000, "scope": "public", "token_type": "bearer"}


def test_a_real_trakt_refresh_is_visible_to_the_next_load(config_base: Path, monkeypatch):
    from providers.auth import _auth_TRAKT as auth

    cfg = cb.load_config()
    trakt = cfg.setdefault("trakt", {})
    trakt.update({"client_id": "cid", "client_secret": "secret", "access_token": "stale-access", "refresh_token": "stale-refresh", "expires_at": 1})
    cb.save_config(cfg)
    assert cb.load_config()["trakt"]["access_token"] == "stale-access"

    monkeypatch.setattr(auth.requests, "post", lambda *a, **k: _FakeTokenResponse())
    result = auth.PROVIDER.refresh()
    assert result["ok"] is True, result

    reloaded = cb.load_config()["trakt"]
    assert reloaded["access_token"] == "fresh-access"
    assert reloaded["refresh_token"] == "fresh-refresh"
    on_disk = json.loads((config_base / "config.json").read_text("utf-8"))
    assert on_disk["trakt"]["access_token"].startswith("enc:v1:")
