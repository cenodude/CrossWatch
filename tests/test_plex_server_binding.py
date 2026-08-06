from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

SERVER_A = "https://192-0-2-10.aaa.plex.direct:32400"
SERVER_B = "https://198-51-100-20.bbb.plex.direct:32401"

SECTIONS_A = '<MediaContainer><Directory key="1" title="A-Movies" type="movie"/></MediaContainer>'
SECTIONS_B = '<MediaContainer><Directory key="9" title="B-Shows" type="show"/></MediaContainer>'


class _Resp:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self.text = text


@pytest.fixture(autouse=True)
def _reset_plex_caches():
    from providers.sync.plex import _utils as u

    u._CACHE["libs"] = {}
    u._LAST_HTTP.clear()
    yield
    u._CACHE["libs"] = {}
    u._LAST_HTTP.clear()


def _patch_transport(monkeypatch, responder):
    from providers.sync.plex import _utils as u

    monkeypatch.setattr(u, "_build_session", lambda token, verify: {"token": token, "verify": verify})
    monkeypatch.setattr(u, "_try_get", responder)


def test_libraries_rebind_pms_token_when_server_url_changes(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    block: dict[str, Any] = {
        "account_token": "ACCOUNT",
        "pms_token": "PMS-A",
        "pms_token_server": SERVER_A,
        "machine_id": "mid-a",
        "server_url": SERVER_B,
        "client_id": "cid",
    }
    cfg = {"plex": block}
    saved: list[int] = []

    def _try_get(session, base, path, timeout=10.0):
        if base.rstrip("/") == SERVER_B and session["token"] == "PMS-B":
            return _Resp(200, SECTIONS_B)
        return _Resp(401, "")

    _patch_transport(monkeypatch, _try_get)
    monkeypatch.setattr(u, "save_config", lambda _c: saved.append(1))
    monkeypatch.setattr(u, "_resolve_verify_from_cfg", lambda *a, **k: True)
    monkeypatch.setattr(u, "_resource_token_for_connection", lambda *a, **k: ("mid-b", "PMS-B"))

    libs = u.fetch_libraries_from_cfg(cfg)

    assert [lib["title"] for lib in libs] == ["B-Shows"]
    assert block["pms_token"] == "PMS-B"
    assert block["machine_id"] == "mid-b"
    assert block["pms_token_server"] == SERVER_B
    assert saved


def test_libraries_fall_back_to_account_token_when_pms_token_rejected(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    block: dict[str, Any] = {
        "account_token": "ACCOUNT",
        "pms_token": "PMS-A",
        "pms_token_server": SERVER_A,
        "machine_id": "mid-a",
        "server_url": SERVER_A,
        "client_id": "cid",
    }
    cfg = {"plex": block}

    def _try_get(session, base, path, timeout=10.0):
        return _Resp(200, SECTIONS_A) if session["token"] == "ACCOUNT" else _Resp(401, "")

    _patch_transport(monkeypatch, _try_get)
    monkeypatch.setattr(u, "save_config", lambda _c: None)
    monkeypatch.setattr(u, "_resolve_verify_from_cfg", lambda *a, **k: True)

    assert [lib["title"] for lib in u.fetch_libraries_from_cfg(cfg)] == ["A-Movies"]


def test_stale_token_is_not_reused_when_rediscovery_fails(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    block: dict[str, Any] = {
        "account_token": "ACCOUNT",
        "pms_token": "PMS-A",
        "pms_token_server": SERVER_A,
        "machine_id": "mid-a",
        "server_url": SERVER_B,
        "client_id": "cid",
    }

    monkeypatch.setattr(u, "_resource_token_for_connection", lambda *a, **k: (None, None))

    token, machine_id, changed = u.ensure_pms_token_bound(block, SERVER_B)

    assert token == ""
    assert machine_id == ""
    assert changed is False
    assert block["pms_token"] == "PMS-A"


def test_library_cache_is_per_server(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    def _try_get(session, base, path, timeout=10.0):
        return _Resp(200, SECTIONS_A if base.rstrip("/") == SERVER_A else SECTIONS_B)

    _patch_transport(monkeypatch, _try_get)

    first = u.fetch_libraries(SERVER_A, "PMS-A", verify=True)
    second = u.fetch_libraries(SERVER_B, "PMS-B", verify=True)

    assert [lib["title"] for lib in first] == ["A-Movies"]
    assert [lib["title"] for lib in second] == ["B-Shows"]


def test_a_failed_read_is_not_cached_as_an_empty_library_list(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    state = {"ok": False}

    def _try_get(session, base, path, timeout=10.0):
        return _Resp(200, SECTIONS_A) if state["ok"] else _Resp(503, "")

    _patch_transport(monkeypatch, _try_get)
    monkeypatch.setattr(u, "_throttle", lambda _p: False)

    assert u.fetch_libraries(SERVER_A, "PMS-A", verify=True) == []
    state["ok"] = True
    assert [lib["title"] for lib in u.fetch_libraries(SERVER_A, "PMS-A", verify=True)] == ["A-Movies"]


def test_insecure_retry_is_not_swallowed_by_the_throttle(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    block: dict[str, Any] = {
        "account_token": "ACCOUNT",
        "pms_token": "PMS-A",
        "pms_token_server": SERVER_A,
        "machine_id": "mid-a",
        "server_url": SERVER_A,
    }
    cfg = {"plex": block}
    attempts: list[bool] = []

    def _try_get(session, base, path, timeout=10.0):
        attempts.append(bool(session["verify"]))
        return _Resp(200, SECTIONS_A) if not session["verify"] else _Resp(495, "")

    _patch_transport(monkeypatch, _try_get)
    monkeypatch.setattr(u, "save_config", lambda _c: None)
    monkeypatch.setattr(u, "_resolve_verify_from_cfg", lambda *a, **k: True)

    libs = u.fetch_libraries_from_cfg(cfg)

    assert [lib["title"] for lib in libs] == ["A-Movies"]
    assert attempts[:2] == [True, False]


RESOURCES_XML = """<MediaContainer>
  <Device name="Owned Server" provides="server" owned="1" clientIdentifier="mid-a" accessToken="TOK-A">
    <Connection protocol="https" uri="{unreachable}" local="0" relay="0"/>
  </Device>
  <Device name="Shared Server" provides="server" owned="0" clientIdentifier="mid-b" accessToken="TOK-B">
    <Connection protocol="https" uri="{reachable}" local="0" relay="0"/>
  </Device>
</MediaContainer>""".format(unreachable=SERVER_A, reachable=SERVER_B)


def test_discovery_skips_unreachable_server_and_picks_a_live_one(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    probed: list[str] = []

    def _probe(uri, token, timeout):
        probed.append(uri)
        return uri == SERVER_B

    monkeypatch.setattr(u, "_probe_identity", _probe)

    assert u._pick_server_url_from_resources(RESOURCES_XML, account_token="ACCOUNT", probe=True) == SERVER_B
    assert probed == [SERVER_A, SERVER_B]


def test_discovery_falls_back_to_top_candidate_when_nothing_answers(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    monkeypatch.setattr(u, "_probe_identity", lambda *a, **k: False)

    assert u._pick_server_url_from_resources(RESOURCES_XML, account_token="ACCOUNT", probe=True) == SERVER_A


def test_candidate_ranking_is_deterministic_not_document_order() -> None:
    from providers.sync.plex import _utils as u

    reversed_xml = """<MediaContainer>
      <Device name="Shared Server" provides="server" owned="0" clientIdentifier="mid-b" accessToken="T">
        <Connection protocol="https" uri="{b}" local="0" relay="0"/>
      </Device>
      <Device name="Owned Server" provides="server" owned="1" clientIdentifier="mid-a" accessToken="T">
        <Connection protocol="https" uri="{a}" local="0" relay="0"/>
      </Device>
    </MediaContainer>""".format(a=SERVER_A, b=SERVER_B)

    for xml in (RESOURCES_XML, reversed_xml):
        assert u._pick_server_url_from_resources(xml, probe=False) == SERVER_A


MULTI_SERVER_XML = """<MediaContainer>
  <Device name="Owned Server" provides="server" owned="1" clientIdentifier="mid-owned" accessToken="T">
    <Connection protocol="https" uri="{owned}" local="0" relay="0"/>
  </Device>
  <Device name="Shared Server" provides="server" owned="0" clientIdentifier="mid-shared" accessToken="T">
    <Connection protocol="https" uri="{shared}" local="0" relay="0"/>
  </Device>
</MediaContainer>""".format(owned=SERVER_A, shared=SERVER_B)


def test_discovery_stays_on_the_bound_server(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    monkeypatch.setattr(u, "_probe_identity", lambda *a, **k: True)

    assert u._pick_server_url_from_resources(MULTI_SERVER_XML, probe=True) == SERVER_A
    assert (
        u._pick_server_url_from_resources(MULTI_SERVER_XML, probe=True, machine_id="mid-shared")
        == SERVER_B
    )


def test_bound_server_is_preferred_before_reachability_order(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    probed: list[str] = []

    def _probe(uri, token, timeout):
        probed.append(uri)
        return True

    monkeypatch.setattr(u, "_probe_identity", _probe)
    u._pick_server_url_from_resources(MULTI_SERVER_XML, probe=True, machine_id="mid-shared")

    assert probed[0] == SERVER_B


def test_unknown_binding_falls_back_to_normal_ranking(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    monkeypatch.setattr(u, "_probe_identity", lambda *a, **k: True)

    assert (
        u._pick_server_url_from_resources(MULTI_SERVER_XML, probe=True, machine_id="gone")
        == SERVER_A
    )


def test_rediscovery_reuses_the_stored_machine_id(monkeypatch) -> None:
    from providers.sync.plex import _utils as u

    block: dict[str, Any] = {
        "account_token": "ACCOUNT",
        "machine_id": "mid-shared",
        "server_url": "",
    }
    cfg = {"plex": block}
    seen: dict[str, Any] = {}

    def _discover(token, timeout=10.0, probe=True, machine_id=""):
        seen["machine_id"] = machine_id
        return SERVER_B

    monkeypatch.setattr(u, "discover_server_url_from_cloud", _discover)
    monkeypatch.setattr(u, "save_config", lambda _c: None)
    monkeypatch.setattr(u, "_resolve_verify_from_cfg", lambda *a, **k: True)
    monkeypatch.setattr(u, "_resource_token_for_connection", lambda *a, **k: ("mid-shared", "PMS-B"))
    _patch_transport(monkeypatch, lambda s, b, p, timeout=10.0: _Resp(200, SECTIONS_B))

    u.fetch_libraries_from_cfg(cfg)

    assert seen["machine_id"] == "mid-shared"
    assert block["server_url"] == SERVER_B


def test_media_override_drops_a_token_bound_to_another_server() -> None:
    import api.authenticationAPI as auth

    cfg = {
        "plex": {
            "account_token": "ACCOUNT",
            "pms_token": "PMS-A",
            "pms_token_server": SERVER_A,
            "machine_id": "mid-a",
            "server_url": SERVER_A,
        }
    }

    block = auth._apply_media_overrides(cfg, "plex", "default", SERVER_B, None)

    assert block["server_url"] == SERVER_B
    assert "pms_token" not in block
    assert "pms_token_server" not in block
    assert "machine_id" not in block


def test_media_override_keeps_the_token_for_the_same_server() -> None:
    import api.authenticationAPI as auth

    cfg = {
        "plex": {
            "account_token": "ACCOUNT",
            "pms_token": "PMS-A",
            "pms_token_server": SERVER_A,
            "machine_id": "mid-a",
            "server_url": SERVER_A,
        }
    }

    block = auth._apply_media_overrides(cfg, "plex", "default", SERVER_A + "/", None)

    assert block["pms_token"] == "PMS-A"
    assert block["machine_id"] == "mid-a"


def test_live_override_does_not_persist_the_probed_server(monkeypatch) -> None:
    import api.authenticationAPI as auth
    from providers.sync.plex import _utils as u

    cfg = {
        "plex": {
            "account_token": "ACCOUNT",
            "pms_token": "PMS-A",
            "pms_token_server": SERVER_A,
            "machine_id": "mid-a",
            "server_url": SERVER_A,
            "client_id": "cid",
        }
    }
    saved: list[int] = []

    def _try_get(session, base, path, timeout=10.0):
        if base.rstrip("/") == SERVER_B and session["token"] == "PMS-B":
            return _Resp(200, SECTIONS_B)
        return _Resp(401, "")

    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda _c: saved.append(1))
    monkeypatch.setattr(u, "save_config", lambda _c: saved.append(1))
    monkeypatch.setattr(u, "_resolve_verify_from_cfg", lambda *a, **k: True)
    monkeypatch.setattr(u, "_resource_token_for_connection", lambda *a, **k: ("mid-b", "PMS-B"))
    _patch_transport(monkeypatch, _try_get)

    app = FastAPI()
    auth.register_auth(app)
    body = TestClient(app).get(f"/api/plex/libraries?server={SERVER_B}").json()

    assert [lib["title"] for lib in body["libraries"]] == ["B-Shows"]
    assert not saved
    assert cfg["plex"]["server_url"] == SERVER_B


def test_libraries_endpoint_reads_the_requested_instance(monkeypatch) -> None:
    import api.authenticationAPI as auth
    from providers.sync.plex import _utils as u

    cfg = {
        "plex": {
            "account_token": "ACCOUNT-A",
            "pms_token": "PMS-A",
            "pms_token_server": SERVER_A,
            "machine_id": "mid-a",
            "server_url": SERVER_A,
            "instances": {
                "2": {
                    "account_token": "ACCOUNT-B",
                    "pms_token": "PMS-B",
                    "pms_token_server": SERVER_B,
                    "machine_id": "mid-b",
                    "server_url": SERVER_B,
                }
            },
        }
    }

    def _try_get(session, base, path, timeout=10.0):
        if base.rstrip("/") == SERVER_A and session["token"] == "PMS-A":
            return _Resp(200, SECTIONS_A)
        if base.rstrip("/") == SERVER_B and session["token"] == "PMS-B":
            return _Resp(200, SECTIONS_B)
        return _Resp(401, "")

    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda _c: None)
    monkeypatch.setattr(u, "save_config", lambda _c: None)
    monkeypatch.setattr(u, "_resolve_verify_from_cfg", lambda *a, **k: True)
    _patch_transport(monkeypatch, _try_get)

    app = FastAPI()
    auth.register_auth(app)
    client = TestClient(app)

    default_libs = client.get("/api/plex/libraries").json()
    inst_libs = client.get("/api/plex/libraries?instance=2").json()

    assert [lib["title"] for lib in default_libs["libraries"]] == ["A-Movies"]
    assert [lib["title"] for lib in inst_libs["libraries"]] == ["B-Shows"]
    assert inst_libs["instance"] == "2"


def test_sync_adapter_drops_a_token_bound_to_another_server() -> None:
    from providers.sync._mod_PLEX import _resolve_pms_binding

    plex_cfg = {
        "account_token": "ACCOUNT",
        "pms_token": "PMS-A",
        "machine_id": "mid-a",
        "pms_token_server": SERVER_A,
    }

    assert _resolve_pms_binding(plex_cfg, {}, SERVER_B) == (None, None)
    assert _resolve_pms_binding(plex_cfg, {}, SERVER_A + "/") == ("PMS-A", "mid-a")
    assert _resolve_pms_binding({"pms_token": "PMS-A"}, {}, SERVER_B) == ("PMS-A", None)


def test_sync_adapter_keeps_a_stale_token_when_it_cannot_be_re_derived() -> None:
    from providers.sync._mod_PLEX import _resolve_pms_binding

    plex_cfg = {"pms_token": "PMS-A", "machine_id": "mid-a", "pms_token_server": SERVER_A}

    assert _resolve_pms_binding(plex_cfg, {}, SERVER_B) == ("PMS-A", "mid-a")


def test_watcher_keeps_a_token_bound_to_the_current_server() -> None:
    from providers.scrobble.plex.watch import _plex_btok

    base, token = _plex_btok(
        {
            "plex": {
                "account_token": "ACCOUNT",
                "pms_token": "PMS-A",
                "pms_token_server": SERVER_A,
                "server_url": SERVER_A,
            }
        }
    )
    assert (base, token) == (SERVER_A, "PMS-A")


def test_watcher_rediscovers_when_the_token_is_bound_elsewhere(monkeypatch) -> None:
    from providers.scrobble.plex import watch

    monkeypatch.setattr(watch, "_try_discover_pms_token", lambda *a, **k: ("PMS-B", "mid-b"))

    base, token = watch._plex_btok(
        {
            "plex": {
                "account_token": "ACCOUNT",
                "pms_token": "PMS-A",
                "pms_token_server": SERVER_A,
                "server_url": SERVER_B,
            }
        }
    )
    assert (base, token) == (SERVER_B, "PMS-B")


def test_watcher_keeps_a_stale_token_when_there_is_no_account_token() -> None:
    from providers.scrobble.plex.watch import _plex_btok

    base, token = _plex_btok(
        {
            "plex": {
                "pms_token": "PMS-A",
                "pms_token_server": SERVER_A,
                "server_url": SERVER_B,
            }
        }
    )
    assert (base, token) == (SERVER_B, "PMS-A")
