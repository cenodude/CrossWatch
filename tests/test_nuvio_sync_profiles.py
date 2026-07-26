# CrossWatch test scripts
from __future__ import annotations

from typing import Any


def test_nuvio_sync_adapter_uses_selected_profile_from_provider_view(monkeypatch) -> None:
    from cw_platform.provider_instances import build_provider_config_view
    from providers.sync._mod_NUVIO import NUVIOModule
    from providers.sync.nuvio._common import pull_watch_progress_rows, selected_profile_id

    cfg: dict[str, Any] = {
        "nuvio": {
            "base_url": "https://api.nuvio.tv",
            "access_token": "default-access",
            "refresh_token": "default-refresh",
            "profile_id": 1,
            "profile_name": "Main",
            "instances": {
                "kid": {
                    "access_token": "kid-access",
                    "refresh_token": "kid-refresh",
                    "profile_id": 2,
                    "profile_name": "Kids",
                }
            },
        }
    }
    view = build_provider_config_view(cfg, "nuvio", "kid")
    monkeypatch.setenv("CW_PAIR_SRC", "NUVIO")
    monkeypatch.setenv("CW_PAIR_SRC_INSTANCE", "kid")

    adapter = NUVIOModule(view)
    calls: list[dict[str, Any]] = []

    class FakeClient:
        def request_json(self, method, path, *, payload=None, refresh=True, retry=True, timeout=20.0):
            calls.append({"method": method, "path": path, "payload": dict(payload or {})})
            return []

    adapter.client = FakeClient()

    assert adapter.instance_id == "kid"
    assert selected_profile_id(adapter) == 2

    pull_watch_progress_rows(adapter, limit=1, max_pages=1)

    assert calls == [
        {
            "method": "POST",
            "path": "/rest/v1/rpc/sync_pull_watch_progress",
            "payload": {"p_profile_id": 2, "p_since_last_watched": 0, "p_limit": 1},
        }
    ]
