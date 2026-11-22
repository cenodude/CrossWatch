# tests/test_anime_mapping_rebuild_process.py
# CrossWatch - Anime mapping rebuild process tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

import json
import sqlite3
import subprocess

import pytest

from cw_platform.anime_mapping import rebuild_process, storage

MAPPINGS = {"mal:1": {"anilist:1": {"1-12": "1-12"}}, "mal:2": {"anilist:2": {"1": "1"}}}
IDENTITY = b"title\tanidb\tanilist\tkitsu\tmyanimelist\tsimkl\nAnime\t1\t1\t10\t1\t100\n"


@pytest.fixture
def dataset(config_base):
    pp = storage.paths("v3")
    pp["root"].mkdir(parents=True)
    pp["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    pp["identity"].write_bytes(IDENTITY)
    return pp


def test_rebuild_runs_in_a_child_process(dataset, monkeypatch):
    monkeypatch.setattr(storage, "rebuild_sqlite_from_mappings", lambda **kwargs: pytest.fail("Rebuilt in the web process"))
    result = rebuild_process.rebuild_index("v3")
    assert result["ok"] and result["source_count"] == 2 and result["identity_count"] == 3
    assert storage.read_state("v3")["index_ready"] is True
    assert storage.query_native_identity("v3", "mal", "1")["simkl"] == "100"


def test_rebuild_replaces_index_held_open_by_a_reader(dataset):
    rebuild_process.rebuild_index("v3")
    assert storage.query_native_identity("v3", "mal", "1")["simkl"] == "100"
    dataset["identity"].write_bytes(IDENTITY.replace(b"\t100\n", b"\t200\n"))
    rebuild_process.rebuild_index("v3")
    assert storage.query_native_identity("v3", "mal", "1")["simkl"] == "200"


def test_rebuilt_index_keeps_wal_and_indexes(dataset):
    rebuild_process.rebuild_index("v3")
    with sqlite3.connect(dataset["db"]) as con:
        assert con.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        names = {row[0] for row in con.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
        assert con.execute("SELECT COUNT(*) FROM mapping_edges").fetchone()[0] == 4
    assert {"idx_mapping_source", "idx_mapping_source_scope", "idx_mapping_target", "idx_show_pairs_tvdb",
            "idx_show_pairs_tmdb", "idx_native_identity_simkl", "idx_native_identity_kitsu"} <= names


def test_child_failure_is_raised(dataset):
    dataset["mappings"].unlink()
    with pytest.raises(rebuild_process.RebuildError, match="FileNotFoundError"):
        rebuild_process.rebuild_index("v3")


def test_child_without_result_is_raised(dataset, monkeypatch):
    monkeypatch.setattr(rebuild_process.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, -9, "", ""))
    with pytest.raises(rebuild_process.RebuildError, match="code -9"):
        rebuild_process.rebuild_index("v3")


def test_launch_failure_rebuilds_in_process(dataset, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("no interpreter")
    monkeypatch.setattr(rebuild_process.subprocess, "run", fail)
    assert rebuild_process.rebuild_index("v3")["ok"]
    assert storage.read_state("v3")["index_ready"] is True
