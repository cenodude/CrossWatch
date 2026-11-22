# services/notification_push.py
# CrossWatch - Web Push delivery for new notifications on opted-in devices
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import base64
import json
import logging
import os
import struct
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlsplit

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from cw_platform import notification_store as store

_LOG = logging.getLogger("crosswatch.notifications")
PUSH_HOSTS = ("fcm.googleapis.com", "push.services.mozilla.com", "push.apple.com", "notify.windows.com")
SUBJECT = "https://github.com/cenodude/CrossWatch"
INTERVAL = 20.0
TTL = 24 * 3600
MAX_PAYLOAD = 3000

_STOP = threading.Event()
_THREAD: threading.Thread | None = None
_THREAD_LOCK = threading.Lock()


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def b64url_decode(value: str) -> bytes:
    text = str(value or "").strip()
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def allowed_endpoint(endpoint: Any) -> bool:
    if not isinstance(endpoint, str) or len(endpoint) > 1024:
        return False
    try:
        parts = urlsplit(endpoint)
    except ValueError:
        return False
    host = (parts.hostname or "").lower()
    if parts.scheme != "https" or parts.username or parts.password or parts.port not in (None, 443):
        return False
    return any(host == base or host.endswith("." + base) for base in PUSH_HOSTS)


def valid_keys(p256dh: Any, auth: Any) -> bool:
    try:
        public = b64url_decode(p256dh)
        secret = b64url_decode(auth)
        ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), public)
    except Exception:
        return False
    return len(public) == 65 and len(secret) == 16


def _private_key() -> ec.EllipticCurvePrivateKey:
    saved = store.meta("vapid_private_key")
    if saved is None:
        fresh = ec.generate_private_key(ec.SECP256R1()).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        ).decode("ascii")
        saved = store.set_meta_once("vapid_private_key", fresh)
    key = serialization.load_pem_private_key(saved.encode("ascii"), password=None)
    if not isinstance(key, ec.EllipticCurvePrivateKey):
        raise ValueError("invalid VAPID key")
    return key


def _raw_public(key: ec.EllipticCurvePublicKey) -> bytes:
    return key.public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)


def public_key() -> str:
    return b64url(_raw_public(_private_key().public_key()))


def vapid_header(endpoint: str, *, now: float | None = None) -> str:
    key = _private_key()
    parts = urlsplit(endpoint)
    claims = {"aud": f"{parts.scheme}://{parts.netloc}", "exp": int((now or time.time()) + 3600), "sub": SUBJECT}
    signing = b64url(json.dumps({"typ": "JWT", "alg": "ES256"}, separators=(",", ":")).encode()) + "." + b64url(json.dumps(claims, separators=(",", ":")).encode())
    r, s = decode_dss_signature(key.sign(signing.encode("ascii"), ec.ECDSA(hashes.SHA256())))
    token = signing + "." + b64url(r.to_bytes(32, "big") + s.to_bytes(32, "big"))
    return f"vapid t={token}, k={b64url(_raw_public(key.public_key()))}"


def _hkdf(salt: bytes, ikm: bytes, info: bytes, length: int) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=length, salt=salt, info=info).derive(ikm)


def encrypt(payload: bytes, p256dh: str, auth: str, *, salt: bytes | None = None, server_key: ec.EllipticCurvePrivateKey | None = None) -> bytes:
    receiver = b64url_decode(p256dh)
    secret = b64url_decode(auth)
    local = server_key or ec.generate_private_key(ec.SECP256R1())
    local_public = _raw_public(local.public_key())
    shared = local.exchange(ec.ECDH(), ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), receiver))
    ikm = _hkdf(secret, shared, b"WebPush: info\x00" + receiver + local_public, 32)
    salt = salt or os.urandom(16)
    cek = _hkdf(salt, ikm, b"Content-Encoding: aes128gcm\x00", 16)
    nonce = _hkdf(salt, ikm, b"Content-Encoding: nonce\x00", 12)
    body = AESGCM(cek).encrypt(nonce, payload + b"\x02", None)
    return salt + struct.pack("!IB", 4096, len(local_public)) + local_public + body


def send(subscription: Mapping[str, Any], message: Mapping[str, Any], *, post: Callable[..., Any] = requests.post) -> int:
    endpoint = str(subscription["endpoint"])
    if not allowed_endpoint(endpoint):
        return 400
    payload = json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode()[:MAX_PAYLOAD]
    body = encrypt(payload, subscription["p256dh"], subscription["auth"])
    headers = {"TTL": str(TTL), "Content-Encoding": "aes128gcm", "Content-Type": "application/octet-stream",
               "Urgency": "normal", "Authorization": vapid_header(endpoint)}
    response = post(endpoint, data=body, headers=headers, timeout=10, allow_redirects=False)
    return int(getattr(response, "status_code", 0) or 0)


def deliver(subscription: Mapping[str, Any], message: Mapping[str, Any], *, post: Callable[..., Any] = requests.post) -> bool:
    endpoint = str(subscription["endpoint"])
    try:
        code = send(subscription, message, post=post)
    except requests.RequestException:
        return False
    if code in (200, 201, 202):
        store.subscription_ok(endpoint)
        return True
    if code in (404, 410) or not allowed_endpoint(endpoint):
        store.remove_subscription(endpoint)
    elif code in (400, 401, 403, 413):
        store.subscription_failed(endpoint)
    return False


def message_for(items: list[Mapping[str, Any]]) -> dict[str, Any]:
    if len(items) == 1:
        item = items[0]
        return {"title": str(item.get("title") or "CrossWatch"), "body": str(item.get("detail") or "")[:300],
                "url": str(item.get("href") or "/"), "tag": str(item.get("key") or "cw-notification")}
    titles = "; ".join(str(item.get("title") or "") for item in items[:4])
    return {"title": f"{len(items)} new CrossWatch notifications", "body": titles[:300], "url": "/", "tag": "cw-notifications"}


def fresh_items(user_id: str, scope: str, endpoint: str, items: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    device = store.push_scope(endpoint)
    marks = store.marks(user_id, ["account", scope])
    sent = set(store.marks(user_id, [device], ("pushed",))[device]["pushed"])
    known = {name: set().union(*marks.get(name, {}).values()) for name in ("account", scope)}
    return [item for item in items if not item.get("quiet") and item["key"] not in sent
            and item["key"] not in known["account" if item["scope"] == "account" else scope]]


def remember(user_id: str, endpoint: str, items: list[Mapping[str, Any]]) -> None:
    keys = [item["key"] for item in items]
    if keys:
        store.add_marks(user_id, store.push_scope(endpoint), "pushed", keys)


def _identity(cfg: Mapping[str, Any], user_id: str) -> tuple[bool, Mapping[str, Any] | None]:
    from api import appAuthAPI

    if not appAuthAPI.auth_required(dict(cfg)):
        return user_id == "local", None
    if user_id == "local":
        return False, None
    identity = appAuthAPI._session_identity(appAuthAPI._cfg_auth(dict(cfg)), {"user_id": user_id})
    return identity is not None, identity


def _request(identity: Mapping[str, Any] | None, view_as: str = "") -> Any:
    from starlette.requests import Request

    from cw_platform.access_policy import VIEW_AS_HEADER

    headers = [(VIEW_AS_HEADER.encode("latin-1"), view_as.encode("latin-1"))] if view_as else []
    scope = {"type": "http", "method": "GET", "path": "/api/profile/notifications", "headers": headers, "query_string": b"", "state": {}}
    request = Request(scope)
    if identity is not None:
        request.state.cw_user = dict(identity)
    return request


def run_once(load_config: Callable[[], Mapping[str, Any]], *, post: Callable[..., Any] = requests.post) -> int:
    from services import notification_center as center

    rows = store.subscriptions()
    if not rows:
        return 0
    cfg = load_config() or {}
    sent = 0
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((row["user_id"], row["scope"], row["user_profile"], row.get("view_as") or ""), []).append(row)
    for (user_id, scope, view_profile, view_as), subs in groups.items():
        known, identity = _identity(cfg, user_id)
        if not known:
            for row in subs:
                store.remove_subscription(row["endpoint"])
            continue
        ctx = center.context(_request(identity, view_as), cfg, user_id, scope, view_profile)
        items = center.collect(ctx)["items"]
        for row in subs:
            fresh = fresh_items(user_id, scope, row["endpoint"], items)
            if fresh and deliver(row, message_for(fresh), post=post):
                remember(user_id, row["endpoint"], fresh)
                sent += 1
    return sent


def baseline(user_id: str, endpoint: str, items: list[Mapping[str, Any]]) -> None:
    remember(user_id, endpoint, [item for item in items if not item.get("quiet")])


def _loop(load_config: Callable[[], Mapping[str, Any]]) -> None:
    while not _STOP.wait(INTERVAL):
        try:
            run_once(load_config)
        except Exception as exc:
            _LOG.warning("notification push failed: %s", type(exc).__name__)


def start(load_config: Callable[[], Mapping[str, Any]]) -> None:
    global _THREAD
    with _THREAD_LOCK:
        if _THREAD is not None and _THREAD.is_alive():
            return
        _STOP.clear()
        _THREAD = threading.Thread(target=_loop, args=(load_config,), name="notification-push", daemon=True)
        _THREAD.start()


def stop() -> None:
    _STOP.set()
