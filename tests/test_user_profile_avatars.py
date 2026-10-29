# tests/test_user_profile_avatars.py
# CrossWatch - Profile pictures set by the administrator
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest

import api.providerInstancesAPI as provider_api
import cw_platform.user_profile_avatars as avatars
from tests.test_crosswatch_profiles import ALICE_PROFILE_ID, BOB_PROFILE_ID, _admin_req, _fake_update

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32


@pytest.fixture()
def store(monkeypatch, tmp_path: Path) -> dict[str, Any]:
    data: dict[str, Any] = {
        "user_profiles": {
            BOB_PROFILE_ID: {"label": "Bob", "instances": {}},
            ALICE_PROFILE_ID: {"label": "Alice", "instances": {}},
        }
    }
    monkeypatch.setattr(avatars, "avatar_dir", lambda: tmp_path)
    monkeypatch.setattr(provider_api, "avatar_dir", lambda: tmp_path)
    monkeypatch.setattr(provider_api, "load_config", lambda: json.loads(json.dumps(data)))
    monkeypatch.setattr(provider_api, "save_config", lambda cfg: data.update(cfg))
    monkeypatch.setattr(provider_api, "update_config", _fake_update(provider_api.load_config, lambda cfg: data.update(cfg)))
    return data


def _upload(profile_id: str) -> Any:
    body = {"data": "data:image/png;base64," + base64.b64encode(PNG).decode(), "content_type": "image/png"}
    return provider_api.api_user_profile_avatar_save(profile_id, body, request=_admin_req())


def test_admin_can_set_and_replace_a_profile_picture(store: dict[str, Any], tmp_path: Path) -> None:
    first = _upload(BOB_PROFILE_ID)
    assert first["ok"] is True and first["avatar_url"].startswith(f"/api/user-profiles/{BOB_PROFILE_ID}/avatar?ts=")
    old_file = store["user_profiles"][BOB_PROFILE_ID]["avatar"]["file"]
    assert (tmp_path / old_file).read_bytes() == PNG

    _upload(BOB_PROFILE_ID)
    new_file = store["user_profiles"][BOB_PROFILE_ID]["avatar"]["file"]
    assert new_file != old_file and not (tmp_path / old_file).exists()
    rows = provider_api._decorate_user_profile_rows(store, provider_api.list_user_profiles(store))
    assert {row["label"]: bool(row["avatar_url"]) for row in rows} == {"Alice": False, "Bob": True}


def test_removing_the_picture_or_the_profile_deletes_the_file(store: dict[str, Any], tmp_path: Path, monkeypatch) -> None:
    import api.appAuthAPI as app_auth_api

    monkeypatch.setattr(app_auth_api, "audit_removed_logins", lambda *args: None)
    _upload(BOB_PROFILE_ID)
    file_a = store["user_profiles"][BOB_PROFILE_ID]["avatar"]["file"]
    assert provider_api.api_user_profile_avatar_delete(BOB_PROFILE_ID, request=_admin_req()) == {"ok": True, "avatar_url": ""}
    assert "avatar" not in store["user_profiles"][BOB_PROFILE_ID] and not (tmp_path / file_a).exists()

    _upload(BOB_PROFILE_ID)
    file_b = store["user_profiles"][BOB_PROFILE_ID]["avatar"]["file"]
    provider_api.api_user_profiles_delete(BOB_PROFILE_ID, request=_admin_req())
    assert not (tmp_path / file_b).exists()


def test_bad_images_and_unknown_profiles_are_rejected(store: dict[str, Any], tmp_path: Path) -> None:
    bad = provider_api.api_user_profile_avatar_save(BOB_PROFILE_ID, {"data": base64.b64encode(b"nope").decode(), "content_type": "image/png"}, request=_admin_req())
    assert bad.status_code == 400
    missing = _upload("22222222222242228222222222222299")
    assert missing.status_code == 404
    assert list(tmp_path.iterdir()) == []


def test_a_signed_in_user_falls_back_to_their_profile_picture(store: dict[str, Any]) -> None:
    import api.appAuthAPI as app_auth_api

    _upload(BOB_PROFILE_ID)
    url = avatars.profile_avatar_url(store, BOB_PROFILE_ID)
    assert app_auth_api._with_profile_avatar(store, {"is_admin": False, "profile_id": BOB_PROFILE_ID, "avatar_url": ""})["avatar_url"] == url
    own = {"is_admin": False, "profile_id": BOB_PROFILE_ID, "avatar_url": "/api/profile/avatar/x?ts=1"}
    assert app_auth_api._with_profile_avatar(store, own) is own
    admin = {"is_admin": True, "profile_id": "", "avatar_url": ""}
    assert app_auth_api._with_profile_avatar(store, admin) is admin
