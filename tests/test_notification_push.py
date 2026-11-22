# /tests/test_notification_push.py
# CrossWatch - Web Push encryption, VAPID signing, push service limits and delivery rules
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import json
import struct

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from api import notificationsAPI
from cw_platform import notification_store as store
from services import notification_center as center
from services import notification_push as push

ENDPOINT = "https://fcm.googleapis.com/fcm/send/abc"


def _browser():
    key = ec.generate_private_key(ec.SECP256R1())
    public = key.public_key().public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
    secret = b"0123456789abcdef"
    return key, push.b64url(public), push.b64url(secret)


def _hkdf(salt, ikm, info, length):
    return HKDF(algorithm=hashes.SHA256(), length=length, salt=salt, info=info).derive(ikm)


def _decrypt(body, key, p256dh, auth):
    salt, rs, size = body[:16], *struct.unpack("!IB", body[16:21])
    sender = body[21:21 + size]
    shared = key.exchange(ec.ECDH(), ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), sender))
    ikm = _hkdf(push.b64url_decode(auth), shared, b"WebPush: info\x00" + push.b64url_decode(p256dh) + sender, 32)
    cek = _hkdf(salt, ikm, b"Content-Encoding: aes128gcm\x00", 16)
    nonce = _hkdf(salt, ikm, b"Content-Encoding: nonce\x00", 12)
    plain = AESGCM(cek).decrypt(nonce, body[21 + size:], None)
    assert rs == 4096 and plain.endswith(b"\x02")
    return plain[:-1]


def test_payload_decrypts_with_the_browser_keys():
    key, p256dh, auth = _browser()
    body = push.encrypt(b'{"title":"Hi"}', p256dh, auth)
    assert _decrypt(body, key, p256dh, auth) == b'{"title":"Hi"}'
    assert push.encrypt(b"x", p256dh, auth) != push.encrypt(b"x", p256dh, auth)


def test_payload_matches_the_rfc_8291_example():
    server = ec.derive_private_key(int.from_bytes(push.b64url_decode("yfWPiYE-n46HLnH0KqZOF1fJJU3MYrct3AELtAQ-oRw"), "big"), ec.SECP256R1())
    body = push.encrypt(b"When I grow up, I want to be a watermelon",
                        "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4",
                        "BTBZMqHH6r4Tts7J_aSIgg", salt=push.b64url_decode("DGv6ra1nlYgDCS1FRnbzlw"), server_key=server)
    assert push.b64url(body) == (
        "DGv6ra1nlYgDCS1FRnbzlwAAEABBBP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A_yl95bQpu6cVPTpK4"
        "Mqgkf1CXztLVBSt2Ks3oZwbuwXPXLWyouBWLVWGNWQexSgSxsj_Qulcy4a-fN")


def test_vapid_header_is_a_valid_es256_token(monkeypatch, tmp_path):
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    header = push.vapid_header("https://fcm.googleapis.com/fcm/send/abc", now=1000)
    token, key = header.removeprefix("vapid t=").split(", k=")
    head, claims, signature = token.split(".")
    assert json.loads(push.b64url_decode(claims)) == {"aud": "https://fcm.googleapis.com", "exp": 1000 + 3600, "sub": push.SUBJECT}
    assert key == push.public_key()
    raw = push.b64url_decode(signature)
    public = ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), push.b64url_decode(key))
    public.verify(encode_dss_signature(int.from_bytes(raw[:32], "big"), int.from_bytes(raw[32:], "big")), f"{head}.{claims}".encode(), ec.ECDSA(hashes.SHA256()))
    assert push.public_key() == key


def test_only_known_push_services_are_contacted():
    for good in (ENDPOINT, "https://updates.push.services.mozilla.com/wpush/v2/x", "https://web.push.apple.com/x", "https://wns2-par02p.notify.windows.com/w/?token=x"):
        assert push.allowed_endpoint(good), good
    for bad in ("http://fcm.googleapis.com/x", "https://fcm.googleapis.com.evil.test/x", "https://192.168.1.10/x", "https://user:pw@fcm.googleapis.com/x",
                "https://fcm.googleapis.com:8443/x", "https://evilpush.apple.com/x", None, "x" * 2000):
        assert not push.allowed_endpoint(bad), bad
    _, p256dh, auth = _browser()
    assert push.valid_keys(p256dh, auth)
    assert not push.valid_keys("abc", auth) and not push.valid_keys(p256dh, "abc")


def _subscribe(user="alice", endpoint=ENDPOINT, scope="home", view_as=""):
    key, p256dh, auth = _browser()
    store.save_subscription(user, scope, "", endpoint, p256dh, auth, "", view_as=view_as)
    return key, p256dh, auth


class _Post:
    def __init__(self, code=201):
        self.code, self.calls = code, []

    def __call__(self, url, data, headers, timeout, allow_redirects):
        self.calls.append((url, data, headers))
        return type("R", (), {"status_code": self.code})()


def _items(*rows):
    out = []
    for source, item_id, scope, quiet in rows:
        item = {"id": item_id, "title": f"T {item_id}", "detail": "D", "href": "/#x", "quiet": quiet}
        out.append({**item, "source": source, "scope": scope, "key": center.notification_key(source, item)})
    return out


def _worker(monkeypatch, tmp_path, items):
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    monkeypatch.setattr(push, "_identity", lambda cfg, user_id: (user_id == "alice", {"id": user_id, "is_admin": True}))
    monkeypatch.setattr(center, "collect", lambda ctx, **kw: {"items": items()})


def test_worker_pushes_each_new_item_once_and_skips_known_ones(monkeypatch, tmp_path):
    current = []
    _worker(monkeypatch, tmp_path, lambda: list(current))
    key, p256dh, auth = _subscribe()
    current[:] = _items(("updates", "old", "account", False))
    push.baseline("alice", ENDPOINT, current)
    post = _Post()
    assert push.run_once(lambda: {}, post=post) == 0 and post.calls == []
    current[:] = _items(("updates", "old", "account", False), ("sync-reviews", "new", "profile", False), ("maintenance", "busy", "account", True))
    assert push.run_once(lambda: {}, post=post) == 1
    url, body, headers = post.calls[0]
    assert url == ENDPOINT and headers["Content-Encoding"] == "aes128gcm" and headers["Authorization"].startswith("vapid t=")
    assert json.loads(_decrypt(body, key, p256dh, auth)) == {"title": "T new", "body": "D", "url": "/#x", "tag": '["sync-reviews","new",""]'}
    assert push.run_once(lambda: {}, post=post) == 0
    store.dismiss("alice", "home", [center.notification_key("sync-reviews", {"id": "again"})])
    current.append(_items(("sync-reviews", "again", "profile", False))[0])
    assert push.run_once(lambda: {}, post=post) == 0


def test_several_new_items_become_one_summary(monkeypatch, tmp_path):
    current = []
    _worker(monkeypatch, tmp_path, lambda: list(current))
    key, p256dh, auth = _subscribe()
    current[:] = _items(("updates", "a", "account", False), ("tracker-check", "b", "account", False))
    post = _Post()
    push.run_once(lambda: {}, post=post)
    message = json.loads(_decrypt(post.calls[0][1], key, p256dh, auth))
    assert message["title"] == "2 new CrossWatch notifications" and message["url"] == "/"


def test_gone_subscriptions_and_removed_users_are_dropped(monkeypatch, tmp_path):
    current = _items(("updates", "a", "account", False))
    _worker(monkeypatch, tmp_path, lambda: list(current))
    _subscribe()
    _subscribe("ghost", "https://fcm.googleapis.com/ghost")
    push.run_once(lambda: {}, post=_Post(410))
    assert store.subscriptions() == []


def test_a_push_that_fails_for_a_moment_is_sent_on_the_next_round(monkeypatch, tmp_path):
    current = _items(("tracker-check", "done", "account", False))
    _worker(monkeypatch, tmp_path, lambda: list(current))
    _subscribe()
    down, up = _Post(503), _Post(201)
    assert push.run_once(lambda: {}, post=down) == 0
    assert store.subscriptions()[0]["failures"] == 0
    assert push.run_once(lambda: {}, post=up) == 1
    assert push.run_once(lambda: {}, post=up) == 0 and len(up.calls) == 1


def test_every_device_gets_account_wide_notices_whatever_its_profile(monkeypatch, tmp_path):
    current = _items(("updates", "0.15.0", "account", False))
    _worker(monkeypatch, tmp_path, lambda: list(current))
    _subscribe(endpoint="https://fcm.googleapis.com/desktop", scope="all")
    _subscribe(endpoint="https://fcm.googleapis.com/phone", scope="home")
    post = _Post()
    assert push.run_once(lambda: {}, post=post) == 2
    assert sorted(call[0] for call in post.calls) == ["https://fcm.googleapis.com/desktop", "https://fcm.googleapis.com/phone"]
    store.remove_subscription("https://fcm.googleapis.com/phone")
    phone = store.push_scope("https://fcm.googleapis.com/phone")
    assert store.marks("alice", [phone], ("pushed",))[phone]["pushed"] == []


def test_background_pushes_keep_the_view_as_profile(monkeypatch, tmp_path):
    seen = []
    _worker(monkeypatch, tmp_path, lambda: [])
    monkeypatch.setattr(center, "collect", lambda ctx, **kw: seen.append(ctx.request.headers.get("x-cw-view-as")) or {"items": []})
    _subscribe(view_as="11111111111141118111111111111111")
    _subscribe(endpoint="https://fcm.googleapis.com/other")
    push.run_once(lambda: {}, post=_Post())
    assert set(seen) == {None, "11111111111141118111111111111111"}


def _client(monkeypatch, tmp_path):
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    monkeypatch.setattr(notificationsAPI, "load_config", lambda: {})
    monkeypatch.setattr(notificationsAPI.center, "collect", lambda ctx, **kw: {"items": _items(("updates", "old", "account", False))})
    app = FastAPI()

    @app.middleware("http")
    async def user(request: Request, call_next):
        request.state.cw_user = {"id": request.headers.get("x-user", "alice"), "is_admin": True}
        return await call_next(request)

    app.include_router(notificationsAPI.router)
    return TestClient(app)


def test_push_api_subscribes_with_a_baseline_and_unsubscribes(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    _, p256dh, auth = _browser()
    status = client.get("/api/profile/notifications/push").json()
    assert status["endpoints"] == [] and len(push.b64url_decode(status["public_key"])) == 65
    bad = {"subscription": {"endpoint": "https://10.0.0.5/push", "keys": {"p256dh": p256dh, "auth": auth}}, "profile": "home"}
    assert client.post("/api/profile/notifications/push", json=bad).status_code == 400
    good = {"subscription": {"endpoint": ENDPOINT, "keys": {"p256dh": p256dh, "auth": auth}}, "profile": "home"}
    assert client.post("/api/profile/notifications/push", json=good).json() == {"ok": True}
    device = store.push_scope(ENDPOINT)
    assert store.marks("alice", [device], ("pushed",))[device]["pushed"] == ['["updates","old",""]']
    assert client.get("/api/profile/notifications/push").json()["endpoints"] == [ENDPOINT]
    assert client.request("DELETE", "/api/profile/notifications/push", json={"endpoint": ENDPOINT}, headers={"x-user": "bob"}).json()["removed"] is False
    assert client.request("DELETE", "/api/profile/notifications/push", json={"endpoint": ENDPOINT}).json()["removed"] is True


def test_subscribing_while_viewing_as_a_profile_saves_that_profile(monkeypatch, tmp_path):
    from cw_platform import config_base

    profile = "11111111111141118111111111111111"
    monkeypatch.setattr(config_base, "load_config", lambda: {"user_profiles": {profile: {"label": "Ann"}}})
    client = _client(monkeypatch, tmp_path)
    _, p256dh, auth = _browser()
    body = {"subscription": {"endpoint": ENDPOINT, "keys": {"p256dh": p256dh, "auth": auth}}, "profile": profile}
    assert client.post("/api/profile/notifications/push", json=body, headers={"x-cw-view-as": profile}).json() == {"ok": True}
    assert store.subscriptions("alice")[0]["view_as"] == profile
    other = {**body, "subscription": {**body["subscription"], "endpoint": "https://fcm.googleapis.com/x"}}
    client.post("/api/profile/notifications/push", json=other, headers={"x-cw-view-as": "22222222222242228222222222222222"})
    assert {row["endpoint"]: row["view_as"] for row in store.subscriptions("alice")}["https://fcm.googleapis.com/x"] == ""


def test_test_push_needs_an_own_subscription(monkeypatch, tmp_path):
    client = _client(monkeypatch, tmp_path)
    _subscribe()
    sent = []
    monkeypatch.setattr(notificationsAPI.push, "deliver", lambda row, message: sent.append(message) or True)
    assert client.post("/api/profile/notifications/push/test", json={"endpoint": ENDPOINT}, headers={"x-user": "bob"}).status_code == 404
    assert client.post("/api/profile/notifications/push/test", json={"endpoint": ENDPOINT}).json() == {"ok": True}
    assert sent[0]["tag"] == "cw-push-test"
