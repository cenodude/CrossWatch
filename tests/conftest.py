# CrossWatch test scripts
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("CW_TRAKT_PACING", "0")


@pytest.fixture()
def config_base(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("CONFIG_BASE", str(tmp_path))
    from cw_platform import config_base as _cb

    monkeypatch.setattr(_cb, "CONFIG", tmp_path, raising=False)
    return tmp_path


@pytest.fixture(autouse=True)
def _isolated_cw_mappings(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> None:
    if request.node.get_closest_marker("real_cw_mappings"):
        return
    from cw_platform.anime_mapping import cw_mappings

    empty = tmp_path_factory.getbasetemp() / "cw_mappings_empty.json"
    if not empty.exists():
        empty.write_bytes(b'{"schema_version": 1, "revision": "2000-01-01.0", "entries": []}')
    monkeypatch.setattr(cw_mappings, "BUNDLED_PATH", empty)
