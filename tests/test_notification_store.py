# /tests/test_notification_store.py
# CrossWatch - Notification read state, dismissals, review waits and the notification API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from api import notificationsAPI
from cw_platform import notification_store as store

URL = "/api/profile/notifications"


def _client(monkeypatch, tmp_path, auth=True, items=()):
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    monkeypatch.setattr(notificationsAPI, "load_config", lambda: {})
    monkeypatch.setattr(notificationsAPI, "auth_required", lambda cfg: auth)
    monkeypatch.setattr(notificationsAPI.center, "collect", lambda ctx, **kw: {"items": list(items), "errors": [], "poll": 10, "signals": {}})
    app = FastAPI()

    @app.middleware("http")
    async def user(request: Request, call_next):
        name = request.headers.get("x-user")
        if name:
            request.state.cw_user = {"id": name, "is_admin": True, "monitor_view": name == "monitor"}
        return await call_next(request)

    app.include_router(notificationsAPI.router)
    return TestClient(app)


def test_marks_keep_order_per_kind_without_duplicates(tmp_path):
    path = tmp_path / "n.sqlite3"
    assert store.marks("alice", ["home"], path=path) == {"home": {"dismissed": [], "seen": []}}
    assert not path.exists()
    assert store.dismiss("alice", "home", ["a", "b"], path) == ["a", "b"]
    assert store.dismiss("alice", "home", ["b", "c", 5, "", "x" * 600], path) == ["a", "b", "c"]
    assert store.add_marks("alice", "home", "seen", ["z"], path) == ["z"]
    assert store.marks("alice", ["home", "account", "home"], path=path) == {
        "home": {"dismissed": ["a", "b", "c"], "seen": ["z"]}, "account": {"dismissed": [], "seen": []}}
    assert store.marks("bob", ["home"], path=path)["home"]["dismissed"] == []


def test_oldest_marks_are_dropped_past_the_limit(tmp_path, monkeypatch):
    path = tmp_path / "n.sqlite3"
    monkeypatch.setattr(store, "MAX_KEYS", 3)
    store.dismiss("alice", "home", ["a", "b"], path)
    assert store.dismiss("alice", "home", ["c", "d"], path) == ["b", "c", "d"]
    assert store.add_marks("alice", "home", "seen", ["z"], path) == ["z"]


def test_review_waits_expire_and_are_viewed_everywhere(tmp_path):
    path = tmp_path / "n.sqlite3"
    store.wait_for_review("alice", "home", "month", "default", "2026-10", "Tracker", path)
    store.wait_for_review("alice", "work", "month", "default", "2026-10", "Tracker", path)
    [row] = store.review_waits("alice", "home", path)
    assert row["ready_revision"] is None and row["label"] == "Tracker"
    store.review_ready("alice", "home", "month", "default", "2026-10", "9", path)
    assert store.review_waits("alice", "home", path)[0]["ready_revision"] == "9"
    assert len(store.review_waits("alice", "work", path, now=row["created_at"] + store.WAIT_SECONDS + 1)) == 1
    assert store.review_waits("alice", "work", path, now=row["created_at"] + store.ABANDON_SECONDS + 1) == []
    store.review_viewed("alice", "month", "default", "2026-10", path)
    assert store.review_waits("alice", "home", path) == []


def test_forget_user_removes_every_kind_of_state(tmp_path):
    path = tmp_path / "n.sqlite3"
    store.dismiss("alice", "home", ["a"], path)
    store.dismiss("bob", "home", ["b"], path)
    store.wait_for_review("alice", "home", "month", "default", "2026-10", "T", path)
    store.save_subscription("alice", "home", "", "https://fcm.googleapis.com/x", "k", "a", "", path)
    store.forget_user("alice", path)
    assert store.marks("alice", ["home"], path=path)["home"]["dismissed"] == []
    assert store.review_waits("alice", "home", path) == [] and store.subscriptions("alice", path) == []
    assert store.marks("bob", ["home"], path=path)["home"]["dismissed"] == ["b"]


def test_subscriptions_are_owned_and_drop_after_repeated_failures(tmp_path):
    path = tmp_path / "n.sqlite3"
    store.save_subscription("alice", "home", "", "https://fcm.googleapis.com/a", "k", "s", "", path)
    assert not store.remove_subscription("https://fcm.googleapis.com/a", "bob", path)
    for _ in range(4):
        store.subscription_failed("https://fcm.googleapis.com/a", path=path)
    store.subscription_ok("https://fcm.googleapis.com/a", path)
    store.subscription_failed("https://fcm.googleapis.com/a", limit=2, path=path)
    assert store.subscriptions("alice", path)[0]["failures"] == 1
    store.subscription_failed("https://fcm.googleapis.com/a", limit=2, path=path)
    assert store.subscriptions(path=path) == []


def test_list_returns_items_with_both_scopes_of_state(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path, items=[{"key": "k", "source": "updates", "scope": "account"}])
    client.post(f"{URL}/dismissed", json={"scope": "account", "keys": ["one"]}, headers={"x-user": "alice"})
    client.post(f"{URL}/seen", json={"scope": "home", "keys": ["two"]}, headers={"x-user": "alice"})
    data = client.get(URL, params={"profile": "home"}, headers={"x-user": "alice"}).json()
    assert data["items"] == [{"key": "k", "source": "updates", "scope": "account"}]
    assert data["state"] == {"account": {"dismissed": ["one"], "seen": []}, "profile": {"dismissed": [], "seen": ["two"]}}
    other = client.get(URL, params={"profile": "home"}, headers={"x-user": "bob"}).json()
    assert other["state"]["account"]["dismissed"] == []
    assert client.get(URL, params={"profile": "account"}, headers={"x-user": "alice"}).status_code == 400


def test_api_rejects_bad_input_monitor_views_and_anonymous_requests(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    assert client.post(f"{URL}/dismissed", json={"scope": "", "keys": ["one"]}, headers={"x-user": "alice"}).status_code == 400
    assert client.post(f"{URL}/seen", json={"scope": ["home"], "keys": ["one"]}, headers={"x-user": "alice"}).status_code == 400
    assert client.post(f"{URL}/dismissed", json={"scope": "home", "keys": ["one"]}, headers={"x-user": "monitor"}).status_code == 403
    assert client.get(URL, params={"profile": "home"}).status_code == 403
    for body in ({"kind": "month", "action": "wait", "instance": "default", "period": "2026-13", "profile": "home"},
                 {"kind": "year", "action": "wait", "instance": "default", "period": "2026", "profile": "home"},
                 {"kind": "month", "action": "wait", "instance": "default", "period": "2026-10", "profile": "account"}):
        assert client.post(f"{URL}/reviews", json=body, headers={"x-user": "alice"}).status_code == 400


def test_review_api_records_and_clears_waits(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    body = {"kind": "month", "action": "wait", "instance": "default", "period": "2026-10", "label": "Tracker", "profile": "home"}
    assert client.post(f"{URL}/reviews", json=body, headers={"x-user": "alice"}).json() == {"ok": True}
    assert store.review_waits("alice", "home")[0]["period"] == "2026-10"
    client.post(f"{URL}/reviews", json={**body, "action": "viewed"}, headers={"x-user": "alice"})
    assert store.review_waits("alice", "home") == []


def test_api_uses_one_local_user_without_authentication(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path, auth=False)
    client.post(f"{URL}/dismissed", json={"scope": "all", "keys": ["one"]})
    assert client.get(URL, params={"profile": "all"}).json()["state"]["profile"]["dismissed"] == ["one"]


def test_storage_errors_do_not_leak_details(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)

    def fail(*args, **kwargs):
        raise OSError("disk path /secret is read only")

    monkeypatch.setattr(notificationsAPI.store, "add_marks", fail)
    response = client.post(f"{URL}/dismissed", json={"scope": "home", "keys": ["one"]}, headers={"x-user": "alice"})
    assert response.status_code == 503
    assert "secret" not in response.text
