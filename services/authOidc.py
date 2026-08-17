# services/authOidc.py
# CrossWatch - OIDC authentication flow management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import base64
import hashlib
import json
import secrets
import time
from urllib.parse import urlencode

import requests
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa, utils as asym_utils
from cryptography.hazmat.primitives import hashes

PENDING_TTL_SEC = 10 * 60
DISCOVERY_TTL_SEC = 15 * 60

_PENDING_FLOWS: dict[str, dict[str, Any]] = {}
_DISCOVERY_CACHE: dict[str, tuple[int, dict[str, Any]]] = {}
_JWKS_CACHE: dict[str, tuple[int, dict[str, Any]]] = {}
_COMPLETED_LINKS: dict[str, dict[str, Any]] = {}


def _now() -> int:
    return int(time.time())


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    raw = str(value or "").encode("ascii")
    raw += b"=" * ((4 - len(raw) % 4) % 4)
    return base64.urlsafe_b64decode(raw)


def _sha256_b64url(value: str) -> str:
    return _b64url(hashlib.sha256(value.encode("ascii")).digest())


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _oidc_cfg(cfg: dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    app_auth = cfg.get("app_auth")
    if not isinstance(app_auth, dict):
        if not create:
            return {}
        app_auth = {}
        cfg["app_auth"] = app_auth
    oidc = app_auth.get("oidc")
    if not isinstance(oidc, dict):
        if not create:
            return {}
        oidc = {}
        app_auth["oidc"] = oidc
    return oidc


def _admin_link(cfg: dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    app_auth = cfg.get("app_auth")
    if not isinstance(app_auth, dict):
        if not create:
            return {}
        app_auth = {}
        cfg["app_auth"] = app_auth
    link = app_auth.get("oidc_identity")
    if isinstance(link, dict):
        return link
    if create:
        app_auth["oidc_identity"] = {}
        return app_auth["oidc_identity"]
    return {}


def _managed_link(raw_user: dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    link = raw_user.get("oidc")
    if isinstance(link, dict):
        return link
    if create:
        raw_user["oidc"] = {}
        return raw_user["oidc"]
    return {}


def _norm_issuer(value: Any) -> str:
    return str(value or "").strip().rstrip("/")


def _norm_scopes(value: Any) -> str:
    scopes = str(value or "openid profile email").strip() or "openid profile email"
    parts = []
    for part in scopes.split():
        if part and part not in parts:
            parts.append(part)
    if "openid" not in parts:
        parts.insert(0, "openid")
    return " ".join(parts)


def sanitize_config(raw: dict[str, Any] | None, previous: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = raw if isinstance(raw, dict) else {}
    previous = previous if isinstance(previous, dict) else {}
    secret = str(raw.get("client_secret") or "").strip()
    if not secret and bool(raw.get("keep_client_secret", False)):
        secret = str(previous.get("client_secret") or "").strip()
    return {
        "enabled": bool(raw.get("enabled", False)),
        "issuer": _norm_issuer(raw.get("issuer")),
        "client_id": str(raw.get("client_id") or "").strip(),
        "client_secret": secret,
        "scopes": _norm_scopes(raw.get("scopes")),
    }


def public_config(cfg: dict[str, Any]) -> dict[str, Any]:
    oidc = _oidc_cfg(cfg)
    return {
        "enabled": bool(oidc.get("enabled", False)),
        "issuer": str(oidc.get("issuer") or ""),
        "client_id": str(oidc.get("client_id") or ""),
        "client_secret_configured": bool(str(oidc.get("client_secret") or "").strip()),
        "scopes": _norm_scopes(oidc.get("scopes")),
        "configured": configured(cfg),
        "login_available": login_available(cfg),
    }


def configured(cfg: dict[str, Any]) -> bool:
    oidc = _oidc_cfg(cfg)
    return bool(oidc.get("enabled") and str(oidc.get("issuer") or "").strip() and str(oidc.get("client_id") or "").strip())


def _link_key(link: dict[str, Any]) -> tuple[str, str]:
    return (_norm_issuer(link.get("iss")), str(link.get("sub") or "").strip())


def identity_matches_link(link: dict[str, Any] | None, identity: dict[str, Any]) -> bool:
    if not isinstance(link, dict):
        return False
    want = _link_key(link)
    got = (_norm_issuer(identity.get("iss")), str(identity.get("sub") or "").strip())
    return bool(want[0] and want[1] and want == got)


def login_available(cfg: dict[str, Any]) -> bool:
    if not configured(cfg):
        return False
    if all(_link_key(_admin_link(cfg))):
        return True
    app_auth = cfg.get("app_auth")
    users = app_auth.get("users") if isinstance(app_auth, dict) else None
    if isinstance(users, dict):
        for raw in users.values():
            if isinstance(raw, dict) and bool(raw.get("enabled", True)) and all(_link_key(_managed_link(raw))):
                return True
    return False


def get_status(cfg: dict[str, Any], raw_user: dict[str, Any] | None = None) -> dict[str, Any]:
    link = _managed_link(raw_user) if isinstance(raw_user, dict) else _admin_link(cfg)
    iss, sub = _link_key(link)
    return {
        **public_config(cfg),
        "linked": bool(iss and sub),
        "linked_issuer": iss,
        "linked_subject": sub,
        "linked_username": str(link.get("username") or "").strip(),
        "linked_email": str(link.get("email") or "").strip(),
        "linked_picture": str(link.get("picture") or "").strip(),
        "linked_at": int(link.get("linked_at") or 0),
    }


def link_identity(cfg: dict[str, Any], identity: dict[str, Any], raw_user: dict[str, Any] | None = None) -> dict[str, Any]:
    link = _managed_link(raw_user, create=True) if isinstance(raw_user, dict) else _admin_link(cfg, create=True)
    link["iss"] = _norm_issuer(identity.get("iss"))
    link["sub"] = str(identity.get("sub") or "").strip()
    link["username"] = str(identity.get("username") or "").strip()
    link["email"] = str(identity.get("email") or "").strip()
    link["picture"] = str(identity.get("picture") or "").strip()
    link["linked_at"] = _now()
    return get_status(cfg, raw_user)


def unlink_identity(cfg: dict[str, Any], raw_user: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(raw_user, dict):
        raw_user.pop("oidc", None)
        return get_status(cfg, raw_user)
    app_auth = cfg.get("app_auth")
    if isinstance(app_auth, dict):
        app_auth.pop("oidc_identity", None)
    return get_status(cfg)


def _discovery(issuer: str) -> dict[str, Any]:
    issuer = _norm_issuer(issuer)
    cached = _DISCOVERY_CACHE.get(issuer)
    now = _now()
    if cached and cached[0] > now:
        return dict(cached[1])
    resp = requests.get(f"{issuer}/.well-known/openid-configuration", timeout=12)
    resp.raise_for_status()
    data = resp.json() or {}
    if _norm_issuer(data.get("issuer")) != issuer:
        raise RuntimeError("OIDC issuer mismatch")
    _DISCOVERY_CACHE[issuer] = (now + DISCOVERY_TTL_SEC, dict(data))
    return data


def _jwks(jwks_uri: str) -> dict[str, Any]:
    uri = str(jwks_uri or "").strip()
    cached = _JWKS_CACHE.get(uri)
    now = _now()
    if cached and cached[0] > now:
        return dict(cached[1])
    resp = requests.get(uri, timeout=12)
    resp.raise_for_status()
    data = resp.json() or {}
    _JWKS_CACHE[uri] = (now + DISCOVERY_TTL_SEC, dict(data))
    return data


def _jwt_parts(token: str) -> tuple[dict[str, Any], dict[str, Any], bytes, bytes]:
    parts = str(token or "").split(".")
    if len(parts) != 3:
        raise RuntimeError("Invalid ID token")
    header = json.loads(_b64url_decode(parts[0]).decode("utf-8"))
    claims = json.loads(_b64url_decode(parts[1]).decode("utf-8"))
    signing_input = f"{parts[0]}.{parts[1]}".encode("ascii")
    signature = _b64url_decode(parts[2])
    return header, claims, signing_input, signature


def _hash_for_alg(alg: str) -> hashes.HashAlgorithm:
    if alg.endswith("384"):
        return hashes.SHA384()
    if alg.endswith("512"):
        return hashes.SHA512()
    return hashes.SHA256()


def _rsa_key(jwk: dict[str, Any]) -> rsa.RSAPublicKey:
    n = int.from_bytes(_b64url_decode(str(jwk.get("n") or "")), "big")
    e = int.from_bytes(_b64url_decode(str(jwk.get("e") or "")), "big")
    return rsa.RSAPublicNumbers(e, n).public_key()


def _ec_key(jwk: dict[str, Any]) -> ec.EllipticCurvePublicKey:
    crv = str(jwk.get("crv") or "")
    curve = {"P-256": ec.SECP256R1(), "P-384": ec.SECP384R1(), "P-521": ec.SECP521R1()}.get(crv)
    if curve is None:
        raise RuntimeError("Unsupported OIDC signing curve")
    x = int.from_bytes(_b64url_decode(str(jwk.get("x") or "")), "big")
    y = int.from_bytes(_b64url_decode(str(jwk.get("y") or "")), "big")
    return ec.EllipticCurvePublicNumbers(x, y, curve).public_key()


def _ecdsa_signature(alg: str, signature: bytes) -> bytes:
    size = {"ES256": 32, "ES384": 48, "ES512": 66}.get(alg, 0)
    if size <= 0 or len(signature) != size * 2:
        return signature
    r = int.from_bytes(signature[:size], "big")
    s = int.from_bytes(signature[size:], "big")
    return asym_utils.encode_dss_signature(r, s)


def _verify_jwt(id_token: str, oidc: dict[str, Any], disco: dict[str, Any], nonce: str) -> dict[str, Any]:
    header, claims, signing_input, signature = _jwt_parts(id_token)
    alg = str(header.get("alg") or "")
    if alg not in {"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}:
        raise RuntimeError("Unsupported OIDC signing algorithm")
    keys = _jwks(str(disco.get("jwks_uri") or "")).get("keys")
    if not isinstance(keys, list):
        raise RuntimeError("OIDC JWKS missing keys")
    kid = str(header.get("kid") or "")
    candidates = [k for k in keys if isinstance(k, dict) and (not kid or str(k.get("kid") or "") == kid)]
    if not candidates:
        raise RuntimeError("OIDC signing key not found")
    last_error: Exception | None = None
    for jwk in candidates:
        try:
            if alg.startswith("RS"):
                _rsa_key(jwk).verify(signature, signing_input, padding.PKCS1v15(), _hash_for_alg(alg))
            else:
                _ec_key(jwk).verify(_ecdsa_signature(alg, signature), signing_input, ec.ECDSA(_hash_for_alg(alg)))
            last_error = None
            break
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise RuntimeError("OIDC ID token signature invalid") from last_error
    issuer = _norm_issuer(oidc.get("issuer"))
    aud = claims.get("aud")
    audiences = aud if isinstance(aud, list) else [aud]
    if _norm_issuer(claims.get("iss")) != issuer:
        raise RuntimeError("OIDC token issuer mismatch")
    if str(oidc.get("client_id") or "") not in [str(x or "") for x in audiences]:
        raise RuntimeError("OIDC token audience mismatch")
    if int(claims.get("exp") or 0) <= _now():
        raise RuntimeError("OIDC token expired")
    if str(claims.get("nonce") or "") != str(nonce or ""):
        raise RuntimeError("OIDC nonce mismatch")
    if not str(claims.get("sub") or "").strip():
        raise RuntimeError("OIDC token subject missing")
    return claims


def _fetch_userinfo(disco: dict[str, Any], access_token: str) -> dict[str, Any]:
    uri = str(disco.get("userinfo_endpoint") or "").strip()
    token = str(access_token or "").strip()
    if not uri or not token:
        return {}
    resp = requests.get(uri, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, timeout=12)
    if not resp.ok:
        return {}
    data = resp.json() or {}
    return data if isinstance(data, dict) else {}


def _identity_from_claims(issuer: str, claims: dict[str, Any], userinfo: dict[str, Any]) -> dict[str, Any]:
    merged = {**claims, **userinfo}
    username = str(merged.get("preferred_username") or merged.get("name") or merged.get("email") or merged.get("sub") or "").strip()
    return {
        "iss": _norm_issuer(issuer),
        "sub": str(claims.get("sub") or "").strip(),
        "username": username,
        "email": str(merged.get("email") or "").strip(),
        "picture": str(merged.get("picture") or "").strip(),
    }


def start_flow(
    cfg: dict[str, Any],
    *,
    intent: str,
    callback_url: str,
    remember_me: bool = False,
    target_user_id: str = "",
    next_url: str = "",
    flow_nonce_hash: str = "",
) -> dict[str, Any]:
    _prune_pending()
    if not configured(cfg):
        raise RuntimeError("OIDC is not configured")
    oidc = _oidc_cfg(cfg)
    disco = _discovery(str(oidc.get("issuer") or ""))
    auth_endpoint = str(disco.get("authorization_endpoint") or "").strip()
    if not auth_endpoint:
        raise RuntimeError("OIDC authorization endpoint missing")
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    verifier = _b64url(secrets.token_bytes(32))
    _PENDING_FLOWS[state] = {
        "intent": str(intent or "").strip(),
        "nonce": nonce,
        "code_verifier": verifier,
        "redirect_uri": str(callback_url or "").strip(),
        "remember_me": bool(remember_me),
        "target_user_id": str(target_user_id or "").strip(),
        "next_url": str(next_url or "").strip(),
        "flow_nonce_hash": str(flow_nonce_hash or "").strip(),
        "expires_at": _now() + PENDING_TTL_SEC,
    }
    params = {
        "client_id": str(oidc.get("client_id") or "").strip(),
        "response_type": "code",
        "scope": _norm_scopes(oidc.get("scopes")),
        "redirect_uri": str(callback_url or "").strip(),
        "state": state,
        "nonce": nonce,
        "code_challenge": _sha256_b64url(verifier),
        "code_challenge_method": "S256",
    }
    return {"ok": True, "state": state, "auth_url": f"{auth_endpoint}?{urlencode(params)}", "expires_at": _PENDING_FLOWS[state]["expires_at"]}


def flow_nonce_hash(state: str) -> str:
    _prune_pending()
    rec = _PENDING_FLOWS.get(str(state or "").strip())
    return str(rec.get("flow_nonce_hash") or "").strip() if isinstance(rec, dict) else ""


def drop_flow(state: str) -> None:
    _PENDING_FLOWS.pop(str(state or "").strip(), None)


def check_callback(cfg: dict[str, Any], *, state: str, code: str) -> dict[str, Any]:
    _prune_pending()
    state = str(state or "").strip()
    rec = _PENDING_FLOWS.pop(state, None)
    if not isinstance(rec, dict):
        return {"ok": False, "error": "OIDC sign-in expired. Start again.", "status_code": 400}
    oidc = _oidc_cfg(cfg)
    disco = _discovery(str(oidc.get("issuer") or ""))
    token_endpoint = str(disco.get("token_endpoint") or "").strip()
    if not token_endpoint:
        return {"ok": False, "error": "OIDC token endpoint missing", "status_code": 502}
    data = {
        "grant_type": "authorization_code",
        "code": str(code or "").strip(),
        "redirect_uri": str(rec.get("redirect_uri") or "").strip(),
        "client_id": str(oidc.get("client_id") or "").strip(),
        "code_verifier": str(rec.get("code_verifier") or "").strip(),
    }
    secret = str(oidc.get("client_secret") or "").strip()
    if secret:
        data["client_secret"] = secret
    resp = requests.post(token_endpoint, data=data, headers={"Accept": "application/json"}, timeout=20)
    resp.raise_for_status()
    token = resp.json() or {}
    id_token = str(token.get("id_token") or "").strip()
    if not id_token:
        return {"ok": False, "error": "OIDC ID token missing", "status_code": 502}
    claims = _verify_jwt(id_token, oidc, disco, str(rec.get("nonce") or ""))
    userinfo = _fetch_userinfo(disco, str(token.get("access_token") or ""))
    return {
        "ok": True,
        "pending": False,
        "intent": str(rec.get("intent") or ""),
        "remember_me": bool(rec.get("remember_me")),
        "target_user_id": str(rec.get("target_user_id") or ""),
        "next_url": str(rec.get("next_url") or ""),
        "identity": _identity_from_claims(str(oidc.get("issuer") or ""), claims, userinfo),
    }


def complete_link(state: str, result: dict[str, Any]) -> None:
    _COMPLETED_LINKS[str(state or "").strip()] = {"expires_at": _now() + PENDING_TTL_SEC, "result": dict(result)}


def consume_link(state: str) -> dict[str, Any]:
    _prune_pending()
    rec = _COMPLETED_LINKS.pop(str(state or "").strip(), None)
    if not isinstance(rec, dict):
        return {"ok": True, "pending": True}
    result = rec.get("result")
    return dict(result) if isinstance(result, dict) else {}


def _prune_pending() -> None:
    now = _now()
    for key in [k for k, v in _PENDING_FLOWS.items() if int(v.get("expires_at") or 0) <= now]:
        _PENDING_FLOWS.pop(key, None)
    for key in [k for k, v in _COMPLETED_LINKS.items() if int(v.get("expires_at") or 0) <= now]:
        _COMPLETED_LINKS.pop(key, None)
