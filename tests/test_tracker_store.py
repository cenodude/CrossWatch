# tests/test_tracker_store.py
# CrossWatch - Local tracker storage contract
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import json
import os
import sqlite3
from io import BytesIO
from zipfile import ZipFile

import pytest

from api import maintenanceAPI
from cw_platform import tracker_store
from cw_platform.tracker_store import TrackerStore, TrackerUnreadable, tracker_context

ITEM = {"type": "movie", "ids": {"tmdb": "1"}, "watched_at": "2026-09-19T12:00:00Z"}


def test_compressed_snapshots_restore_and_export_as_json(tmp_path):
    store = TrackerStore(tmp_path)
    items = {f"tmdb:{n}": {**ITEM, "title": "Amélie 日本語", "ids": {"tmdb": str(n)}} for n in range(1000)}
    store.add_restore_point("history", items)
    point = store.restore_points("history")[0]
    with sqlite3.connect(store.db_path) as conn:
        data = conn.execute("SELECT data FROM restore_points").fetchone()[0]
    assert isinstance(data, bytes)
    assert len(data) < len(json.dumps(items).encode()) / 4
    assert point.size == len(data)
    assert store.read_restore_point("history", point.name)["items"] == items
    assert store.load_items("history", point.name)["items"] == items
    assert store.read_state("history").raw["items"] == items
    with ZipFile(BytesIO(store.export_zip())) as archive:
        assert json.loads(archive.read(f"snapshots/{point.name}"))["items"] == items
    target = TrackerStore(tmp_path / "imported")
    target.import_zip(BytesIO(store.export_zip()))
    assert target.read_restore_point("history", point.name)["items"] == items


def test_unchanged_snapshots_reuse_storage_and_still_apply_retention(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path, scope="pair-a")
    now = 1_800_000_000_000_000_000
    monkeypatch.setattr(tracker_store.time, "time_ns", lambda: now)
    store.add_restore_point("history", {"old": ITEM})
    now += 2_000_000_000
    store.add_restore_point("history", {"current": ITEM})
    name = store.restore_points("history")[-1].name
    with sqlite3.connect(store.db_path) as conn:
        before = conn.execute("SELECT data FROM restore_points WHERE name=?", (name,)).fetchone()[0]
    now += 40 * 86400_000_000_000
    store.add_restore_point("history", {"current": dict(reversed(list(ITEM.items())))})
    assert [p.name for p in store.restore_points("history")] == [name]
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT data FROM restore_points").fetchone()[0] == before
    other = TrackerStore(tmp_path, scope="pair-b")
    other.add_restore_point("history", {"current": ITEM})
    assert len(other.restore_points("history")) == 1


def test_changed_snapshot_within_reuse_window_stays_readable(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    now = 1_800_000_000_000_000_000
    monkeypatch.setattr(tracker_store.time, "time_ns", lambda: now)
    store.add_restore_point("history", {"one": ITEM}, reuse_window=60)
    now += 2_000_000_000
    store.add_restore_point("history", {"two": ITEM}, reuse_window=60)
    points = store.restore_points("history")
    assert len(points) == 1
    assert store.read_restore_point("history", points[0].name)["items"] == {"two": ITEM}


def test_snapshot_migration_resumes_and_preserves_original_metadata(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    conn = store._connect()
    original = json.dumps({"ts": 123, "items": {"tmdb:1": ITEM}})
    for n in range(3):
        conn.execute(
            "INSERT INTO restore_points(scope, feature, name, ts, created_ns, data) VALUES('pair-a', 'history', ?, ?, ?, ?)",
            (f"{n}-history.json", n, n + 100, original),
        )
    pack = tracker_store._pack_snapshot
    calls = 0

    def interrupt(text):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("interrupted")
        return pack(text)

    monkeypatch.setattr(tracker_store, "_pack_snapshot", interrupt)
    with pytest.raises(RuntimeError, match="interrupted"):
        store._migrate_snapshots(conn)
    assert conn.execute("SELECT count(*) FROM restore_points WHERE typeof(data)='blob'").fetchone()[0] == 1
    monkeypatch.setattr(tracker_store, "_pack_snapshot", pack)
    store._migrate_snapshots(conn)
    rows = conn.execute("SELECT scope, name, ts, created_ns, data FROM restore_points ORDER BY id").fetchall()
    assert [(s, name, ts, ns) for s, name, ts, ns, _ in rows] == [
        ("pair-a", f"{n}-history.json", n, n + 100) for n in range(3)
    ]
    assert all(tracker_store._snapshot_text(data) == original for *_, data in rows)
    assert conn.execute("SELECT count(*) FROM restore_points WHERE typeof(data)='text'").fetchone()[0] == 0
    conn.close()


def test_open_leaves_migration_and_compaction_to_background(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    conn = store._connect()
    original = json.dumps({"ts": 1, "items": {"tmdb:1": {**ITEM, "title": "x" * 2_000_000}}})
    conn.execute(
        "INSERT INTO restore_points(scope, feature, name, ts, created_ns, data) VALUES('', 'history', 'old-history.json', 1, 1, ?)",
        (original,),
    )
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    before = store.db_path.stat().st_size
    conn.close()
    monkeypatch.setattr(tracker_store, "_SNAPSHOT_COMPACT_MIN", 1024)
    assert store.read_restore_point("history", "old-history.json") == json.loads(original)
    assert store.db_path.stat().st_size == before
    conn = store._connect()
    assert conn.execute("SELECT typeof(data) FROM restore_points").fetchone()[0] == "text"
    store._migrate_snapshots(conn)
    store._compact_snapshots(conn)
    conn.close()
    assert store.db_path.stat().st_size < before / 4


def test_compaction_defers_without_space_and_retries_later(tmp_path, monkeypatch, caplog):
    from types import SimpleNamespace

    store = TrackerStore(tmp_path)
    conn = store._connect()
    conn.execute("INSERT INTO docs(scope, name, data, updated_ns) VALUES('', 'large', ?, 1)", ("x" * 2_000_000,))
    conn.execute("DELETE FROM docs")
    monkeypatch.setattr(tracker_store, "_SNAPSHOT_COMPACT_MIN", 1024)
    disk_usage = tracker_store.shutil.disk_usage
    monkeypatch.setattr(tracker_store.shutil, "disk_usage", lambda _: SimpleNamespace(free=0))
    free = conn.execute("PRAGMA freelist_count").fetchone()[0]
    store._compact_snapshots(conn)
    assert "snapshot_compaction_deferred" in caplog.text
    assert conn.execute("PRAGMA freelist_count").fetchone()[0] == free
    monkeypatch.setattr(tracker_store.shutil, "disk_usage", disk_usage)
    store._compact_snapshots(conn)
    assert conn.execute("PRAGMA freelist_count").fetchone()[0] < free
    conn.close()


def test_compaction_is_throttled_and_does_not_interrupt_transactions(tmp_path, monkeypatch):
    store = TrackerStore(tmp_path)
    conn = store._connect()
    store._compact_snapshots(conn)
    conn.execute("INSERT INTO docs(scope, name, data, updated_ns) VALUES('', 'large', ?, 1)", ("x" * 2_000_000,))
    conn.execute("DELETE FROM docs")
    monkeypatch.setattr(tracker_store, "_SNAPSHOT_COMPACT_MIN", 1024)
    free = conn.execute("PRAGMA freelist_count").fetchone()[0]
    store._compact_snapshots(conn)
    assert conn.execute("PRAGMA freelist_count").fetchone()[0] == free
    conn.execute("DELETE FROM meta WHERE key='snapshot_compact_checked'")
    with store._write(conn):
        store._compact_snapshots(conn)
        assert conn.in_transaction
    assert conn.execute("PRAGMA freelist_count").fetchone()[0] == free
    conn.close()


@pytest.mark.parametrize("data", [b"CWSP\x01broken", b"CWSP\x02unknown"])
def test_corrupt_or_unknown_compression_fails_restore(tmp_path, data):
    store = TrackerStore(tmp_path)
    store.write_state("watchlist", {})
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(
            "INSERT INTO restore_points(scope, feature, name, ts, created_ns, data) VALUES('', 'history', 'bad-history.json', 1, 1, ?)",
            (data,),
        )
    with pytest.raises(TrackerUnreadable):
        store.read_restore_point("history", "bad-history.json")
    with pytest.raises(TrackerUnreadable):
        store.read_state("history")
    store.add_restore_point("history", {"tmdb:1": ITEM})
    assert store.read_state("history").raw["items"] == {"tmdb:1": ITEM}


def test_state_round_trips_and_missing_state_reads_as_none(tmp_path):
    store = TrackerStore(tmp_path)
    assert store.read_state("history") is None
    assert store.write_state("history", {"tmdb:1": ITEM}, ts=5)
    assert store.read_state("history").raw == {"ts": 5, "items": {"tmdb:1": ITEM}}
    assert store.has_state("history")


def test_unreadable_state_raises_instead_of_reading_empty(tmp_path):
    (tmp_path / "history.json").write_text("{broken", "utf-8")
    with pytest.raises(TrackerUnreadable) as exc:
        TrackerStore(tmp_path).read_state("history")
    assert exc.value.names == ["history.json"]
    assert str(exc.value) == "crosswatch_history_state_unreadable:history.json"


def test_missing_state_falls_back_to_the_newest_restore_point_only_when_allowed(tmp_path):
    store = TrackerStore(tmp_path)
    store.add_restore_point("history", {"tmdb:1": ITEM})
    found = store.read_state("history")
    assert found.raw["items"] == {"tmdb:1": ITEM}
    assert found.restored_from == store.restore_points("history")[0].name
    assert store.read_state("history", snapshot_fallback=False) is None


def test_restore_points_keep_the_newest_within_the_limit(tmp_path):
    store = TrackerStore(tmp_path)
    snaps = tmp_path / "snapshots"
    snaps.mkdir()
    for n in range(3):
        path = snaps / f"2026010{n + 1}T000000Z-history.json"
        path.write_text(json.dumps({"ts": n, "items": {}}), "utf-8")
        os.utime(path, (1_700_000_000 + n, 1_700_000_000 + n))
    store.add_restore_point("history", {"tmdb:1": ITEM}, retention_days=0, max_snapshots=2)
    names = [p.name for p in store.restore_points("history")]
    assert len(names) == 2
    assert "20260103T000000Z-history.json" in names
    assert store.read_restore_point("history", names[-1])["items"] == {"tmdb:1": ITEM}


def test_restore_point_reads_refuse_unknown_names(tmp_path):
    store = TrackerStore(tmp_path)
    with pytest.raises(ValueError):
        store.read_restore_point("history", "../config.json")
    with pytest.raises(ValueError):
        store.load_items("history", "../config.json")


def test_pair_scoped_state_stays_in_its_own_file(tmp_path):
    TrackerStore(tmp_path).write_state("watchlist", {"tmdb:1": ITEM})
    scoped = TrackerStore(tmp_path, scope="pair-a", chained=False)
    assert scoped.read_state("watchlist").raw["items"] == {"tmdb:1": ITEM}
    scoped.write_state("watchlist", {})
    assert TrackerStore(tmp_path).read_state("watchlist").raw["items"] == {"tmdb:1": ITEM}
    assert scoped.read_state("watchlist").raw["items"] == {}


def test_profile_roots_follow_the_configured_instances(tmp_path):
    cfg = {"crosswatch": {"root_dir": "tracker", "instances": {"CW-P01": {}}}}
    assert tracker_context(cfg, tmp_path) == ("default", tmp_path / "tracker")
    assert tracker_context(cfg, tmp_path, "CW-P01") == ("CW-P01", tmp_path / "tracker" / "profiles" / "CW-P01")
    assert tracker_context(cfg, tmp_path, "../outside") == ("default", tmp_path / "tracker")


def test_restore_points_endpoint_lists_names_per_feature_for_the_profile(tmp_path, monkeypatch):
    base = tmp_path / ".cw_provider"
    profile = base / "profiles" / "CW-P01"
    TrackerStore(base).add_restore_point("history", {"tmdb:1": ITEM})
    TrackerStore(profile).add_restore_point("ratings", {"tmdb:1": ITEM})
    (tmp_path / "config.json").write_text(json.dumps({"crosswatch": {"root_dir": str(base), "instances": {"CW-P01": {}}}}), "utf-8")
    monkeypatch.setattr(maintenanceAPI, "_cw", lambda: (tmp_path / "cache", tmp_path, tmp_path / ".cw_state", None, None, None))

    default = maintenanceAPI.crosswatch_tracker_restore_points(provider_instance="default")
    scoped = maintenanceAPI.crosswatch_tracker_restore_points(provider_instance="CW-P01")

    assert len(default["restore_points"]["history"]) == 1
    assert default["restore_points"]["ratings"] == []
    assert scoped["provider_instance"] == "CW-P01"
    assert scoped["restore_points"]["history"] == []
    assert len(scoped["restore_points"]["ratings"]) == 1


def _legacy(root, name, payload, mtime):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), "utf-8")
    os.utime(path, (mtime, mtime))
    return path


def test_first_open_imports_json_files_and_moves_them_to_a_backup(tmp_path):
    _legacy(tmp_path, "history.json", {"ts": 7, "items": {"tmdb:1": ITEM}}, 1_700_000_100)
    _legacy(tmp_path, "watchlist.pair-a.json", {"ts": 8, "items": {"tmdb:2": ITEM}}, 1_700_000_200)
    _legacy(tmp_path, "history.unresolved.json", {"items": {"x": {}}}, 1_700_000_100)
    _legacy(tmp_path, "playlists.json", {"version": 1, "lists": {}}, 1_700_000_100)
    _legacy(tmp_path, "snapshots/20260101T000000Z-history.json", {"ts": 1, "items": {}}, 1_700_000_000)
    _legacy(tmp_path, "snapshots/pair-a/20260102T000000Z-watchlist.json", {"ts": 2, "items": {}}, 1_700_000_050)
    (tmp_path / "profiles" / "CW-P01").mkdir(parents=True)

    store = TrackerStore(tmp_path)

    assert store.read_current("history") == {"ts": 7, "items": {"tmdb:1": ITEM}}
    assert store.read_meta("history.unresolved") == {"items": {"x": {}}}
    assert store.read_playlists() == {"version": 1, "lists": {}}
    assert store.restore_points("history") == []
    scoped = TrackerStore(tmp_path, scope="pair-a", chained=False)
    assert scoped.read_current("watchlist")["items"] == {"tmdb:2": ITEM}
    assert scoped.restore_points("watchlist") == []
    assert not list(tmp_path.glob("*.json"))
    assert not (tmp_path / "snapshots").exists()
    backup = next(tmp_path.glob("json-backup-*"))
    assert (backup / "history.json").exists()
    assert (backup / "snapshots" / "pair-a" / "20260102T000000Z-watchlist.json").exists()
    assert (tmp_path / "profiles" / "CW-P01").is_dir()


def test_newer_pair_state_wins_for_a_chained_source_after_import(tmp_path):
    _legacy(tmp_path, "watchlist.downstream.json", {"items": {"tmdb:1": ITEM}}, 1_700_000_100)
    _legacy(tmp_path, "watchlist.upstream.json", {"items": {"tmdb:2": ITEM}}, 1_700_000_200)

    assert set(TrackerStore(tmp_path, scope="downstream", chained=True).read_state("watchlist").raw["items"]) == {"tmdb:2"}
    assert set(TrackerStore(tmp_path, scope="downstream", chained=False).read_state("watchlist").raw["items"]) == {"tmdb:1"}


@pytest.mark.parametrize("name", ["history.json", "playlists.json"])
def test_a_corrupt_json_file_blocks_the_import_and_stays_in_place(tmp_path, name):
    (tmp_path / name).write_text("{broken", "utf-8")
    _legacy(tmp_path, "ratings.json", {"items": {"tmdb:1": ITEM}}, 1_700_000_100)

    with pytest.raises(TrackerUnreadable):
        TrackerStore(tmp_path).read_state("ratings")

    assert (tmp_path / name).exists()
    assert (tmp_path / "ratings.json").exists()
    (tmp_path / name).write_text(json.dumps({"items": {}}), "utf-8")
    assert TrackerStore(tmp_path).read_state("ratings").raw["items"] == {"tmdb:1": ITEM}


def test_reading_an_empty_root_creates_no_database(tmp_path):
    store = TrackerStore(tmp_path / "missing")
    assert store.read_state("history") is None
    assert store.restore_points("history") == []
    assert store.stamp(["history"]) == [("history", 0, 0)]
    assert not (tmp_path / "missing").exists()


def test_profile_with_its_own_root_dir_uses_it(tmp_path):
    cfg = {"crosswatch": {"root_dir": str(tmp_path / "base"), "instances": {"CW-P02": {"root_dir": str(tmp_path / "own")}}}}
    assert tracker_context(cfg, tmp_path, "CW-P02") == ("CW-P02", tmp_path / "own")


def test_failed_writes_are_logged_not_raised(tmp_path, caplog):
    blocked = tmp_path / "not-a-folder"
    blocked.write_text("x", "utf-8")
    store = TrackerStore(blocked)
    assert store.write_state("history", {"tmdb:1": {"type": "movie"}}) is False
    store.add_restore_point("history", {"tmdb:1": {"type": "movie"}})
    assert "store_write_failed" in caplog.text


@pytest.mark.parametrize("feature", ["watchlist", "collection", "progress", "ratings", "history"])
def test_legacy_list_states_keep_real_keys_so_removes_still_work(tmp_path, feature):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    item = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": "949"}, "rating": 8,
            "watched_at": "2026-01-01T00:00:00Z", "collected_at": "2026-01-01T00:00:00Z",
            "progress_percent": 40, "progress_ms": 4000, "duration_ms": 10000}
    (tmp_path / f"{feature}.json").write_text(json.dumps([item]), "utf-8")
    expected = "tmdb:949@1767225600" if feature == "history" else "tmdb:949"
    assert list(TrackerStore(tmp_path).read_current(feature)["items"]) == [expected]
    adapter = CROSSWATCHModule({"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)}})
    assert adapter.remove(feature, [item])["count"] == 1
    assert TrackerStore(tmp_path).read_current(feature)["items"] == {}


def test_a_corrupt_snapshot_blocks_the_move_instead_of_hiding_the_tracker(tmp_path):
    (tmp_path / "snapshots").mkdir()
    (tmp_path / "snapshots" / "20261001T000000Z-history.json").write_text("{broken", "utf-8")
    with pytest.raises(TrackerUnreadable) as exc:
        TrackerStore(tmp_path).read_state("history")
    assert exc.value.names == ["snapshots/20261001T000000Z-history.json"]
    assert (tmp_path / "snapshots" / "20261001T000000Z-history.json").is_file()
    assert not list(tmp_path.glob("json-backup-*"))


def test_a_corrupt_row_is_unreadable_everywhere(tmp_path):
    import sqlite3

    store = TrackerStore(tmp_path)
    store.write_state("history", {"tmdb:1": {"type": "movie", "title": "A", "ids": {"tmdb": "1"}}})
    with sqlite3.connect(tmp_path / "tracker.db") as conn:
        conn.execute("UPDATE items SET data='{bad' WHERE item_key='tmdb:1'")
    for read in (lambda: store.query("history"), lambda: store.load_items("history"), lambda: list(store.iter_records())):
        with pytest.raises(TrackerUnreadable):
            read()


def test_legacy_list_entries_without_identity_keep_their_position(tmp_path):
    item = {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}}
    (tmp_path / "watchlist.json").write_text(json.dumps([item, {"type": "movie"}]), "utf-8")
    assert set(TrackerStore(tmp_path).read_current("watchlist")["items"]) == {"tmdb:949", "1"}


def test_legacy_entries_that_are_not_records_block_the_import(tmp_path):
    (tmp_path / "watchlist.json").write_text(json.dumps([{"type": "movie", "ids": {"tmdb": "949"}}, "junk"]), "utf-8")
    with pytest.raises(TrackerUnreadable):
        TrackerStore(tmp_path).read_current("watchlist")
    assert (tmp_path / "watchlist.json").exists()


@pytest.mark.parametrize(("rewatches", "count"), [(True, 2), (False, 1)])
def test_legacy_history_list_keeps_every_watch_for_rewatch_mode(tmp_path, rewatches, count):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    movie = {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}}
    watches = [{**movie, "watched_at": "2026-01-01T00:00:00Z"}, {**movie, "watched_at": "2026-02-01T00:00:00Z"}]
    (tmp_path / "history.json").write_text(json.dumps(watches), "utf-8")
    assert len(TrackerStore(tmp_path).read_current("history")["items"]) == 2
    adapter = CROSSWATCHModule({"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)}, "_cw_history_rewatches": rewatches})
    assert len(adapter.build_index("history")) == count


def test_a_write_without_rewatches_only_replaces_the_latest_watch(tmp_path):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    cfg = {"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)}}
    heat = {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}}
    arrival = {"type": "movie", "title": "Arrival", "ids": {"tmdb": "329865"}}
    watches = [{**heat, "watched_at": "2026-01-01T00:00:00Z"}, {**heat, "watched_at": "2026-02-01T00:00:00Z"}]
    CROSSWATCHModule({**cfg, "_cw_history_rewatches": True}).add("history", watches)
    adapter = CROSSWATCHModule(cfg)

    adapter.add("history", [{**arrival, "watched_at": "2026-03-01T00:00:00Z"}])
    adapter.add("history", [{**heat, "watched_at": "2026-04-01T00:00:00Z"}])
    adapter.add("history", [{**heat, "watched_at": "2026-04-01T00:00:00Z"}])
    adapter.add("history", [{**heat, "watched_at": "2025-12-01T00:00:00Z"}])

    stored = TrackerStore(tmp_path).read_current("history")["items"].values()
    assert sorted((item["ids"]["tmdb"], item["watched_at"]) for item in stored) == [
        ("329865", "2026-03-01T00:00:00Z"),
        ("949", "2026-01-01T00:00:00Z"),
        ("949", "2026-04-01T00:00:00Z"),
    ]
    assert len(adapter.build_index("history")) == 2


@pytest.mark.parametrize("newest_key", ["tmdb:949", "tmdb:949@1775001600"])
def test_a_read_without_rewatches_returns_the_newest_watch(tmp_path, newest_key):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    heat = {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}}
    TrackerStore(tmp_path).write_state("history", {
        "tmdb:949@1767225600": {**heat, "watched_at": "2026-01-01T00:00:00Z"},
        newest_key: {**heat, "watched_at": "2026-04-01T00:00:00Z"},
        "tmdb:949@1769904000": {**heat, "watched_at": "2026-02-01T00:00:00Z"},
    })
    adapter = CROSSWATCHModule({"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)}})
    assert [item["watched_at"] for item in adapter.build_index("history").values()] == ["2026-04-01T00:00:00Z"]


def test_concurrent_completions_inside_the_rewatch_window_store_one_watch(tmp_path):
    import threading

    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    cfg = {"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)},
           "_cw_history_rewatches": True, "_cw_history_rewatch_window": 12 * 3600}
    heat = {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}}
    gate = threading.Barrier(2)
    results: list[dict] = []

    def complete(at):
        adapter = CROSSWATCHModule(cfg)
        gate.wait()
        results.append(adapter.add("history", [{**heat, "watched_at": at}]))

    workers = [threading.Thread(target=complete, args=(at,)) for at in ("2026-03-01T20:00:00Z", "2026-03-01T20:00:01Z")]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(10)
    assert len(TrackerStore(tmp_path).read_current("history")["items"]) == 1
    assert sorted(bool(result.get("window_merged")) for result in results) == [False, True]


def test_rewatch_window_matches_any_shared_id(tmp_path):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    adapter = CROSSWATCHModule({"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)},
                                "_cw_history_rewatches": True, "_cw_history_rewatch_window": 12 * 3600})
    heat = {"type": "movie", "title": "Heat", "ids": {"imdb": "tt0113277"}}
    adapter.add("history", [{**heat, "watched_at": "2026-03-01T20:00:00Z"}])
    result = adapter.add("history", [{**heat, "ids": {"imdb": "tt0113277", "tmdb": "949"}, "watched_at": "2026-03-01T21:00:00Z"}])
    assert result.get("window_merged") == 1
    assert len(TrackerStore(tmp_path).read_current("history")["items"]) == 1


def test_rewatch_window_keeps_other_episodes_with_show_level_ids(tmp_path):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    adapter = CROSSWATCHModule({"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)},
                                "_cw_history_rewatches": True, "_cw_history_rewatch_window": 12 * 3600})
    for number, at in ((1, "2026-03-01T20:00:00Z"), (2, "2026-03-01T21:00:00Z")):
        result = adapter.add("history", [{"type": "episode", "ids": {"tmdb": "30"}, "season": 1, "episode": number, "watched_at": at}])
        assert not result.get("window_merged")
    assert len(TrackerStore(tmp_path).read_current("history")["items"]) == 2


def _window_adapter(tmp_path):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    return CROSSWATCHModule({"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)},
                             "_cw_history_rewatches": True, "_cw_history_rewatch_window": 12 * 3600})


def test_rewatch_window_ignores_server_local_ids(tmp_path):
    adapter = _window_adapter(tmp_path)
    adapter.add("history", [{"type": "movie", "title": "Heat", "ids": {"plex": "123", "tmdb": "949"}, "watched_at": "2026-03-01T20:00:00Z"}])
    result = adapter.add("history", [{"type": "movie", "title": "Arrival", "ids": {"plex": "123", "tmdb": "329865"}, "watched_at": "2026-03-01T21:00:00Z"}])
    assert not result.get("window_merged")
    assert len(TrackerStore(tmp_path).read_current("history")["items"]) == 2


def test_rewatch_window_keeps_episode_ids_apart_from_show_ids(tmp_path):
    adapter = _window_adapter(tmp_path)
    adapter.add("history", [{"type": "episode", "ids": {"tmdb": "30"}, "season": 1, "episode": 1, "watched_at": "2026-03-01T20:00:00Z"}])
    result = adapter.add("history", [{"type": "episode", "show_ids": {"tmdb": "30"}, "ids": {"tmdb": "555"}, "season": 1, "episode": 1,
                                      "watched_at": "2026-03-01T21:00:00Z"}])
    assert not result.get("window_merged")
    assert len(TrackerStore(tmp_path).read_current("history")["items"]) == 2


def test_a_tracker_edit_waits_for_an_adapter_write_instead_of_being_undone_by_it(tmp_path, monkeypatch):
    import threading
    import time

    from providers.sync import _mod_CROSSWATCH
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    heat = {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}, "watched_at": "2026-01-01T00:00:00Z"}
    arrival = {"type": "movie", "title": "Arrival", "ids": {"tmdb": "329865"}, "watched_at": "2026-01-02T00:00:00Z"}
    TrackerStore(tmp_path).write_state("history", {"tmdb:949": heat})
    history = _mod_CROSSWATCH._FEATURES["history"]
    real_load = history._load_state
    ui = {}

    def load_then_ui_deletes(adapter):
        state = real_load(adapter)
        ui["thread"] = threading.Thread(
            target=lambda: TrackerStore(tmp_path).change_items("history", [("tmdb:949", None, None)], action="delete")
        )
        ui["thread"].start()
        time.sleep(0.3)
        ui["blocked"] = ui["thread"].is_alive()
        return state

    monkeypatch.setattr(history, "_load_state", load_then_ui_deletes)
    adapter = CROSSWATCHModule({"CrossWatch": {"connected": True, "enabled": True, "root_dir": str(tmp_path)}})
    assert adapter.add("history", [arrival])["ok"] is True
    ui["thread"].join(10)
    assert ui["blocked"] is True
    assert set(TrackerStore(tmp_path).read_current("history")["items"]) == {"tmdb:329865"}


def test_a_corrupt_sidecar_is_logged_and_kept_in_the_backup(tmp_path, caplog):
    (tmp_path / "history.json").write_text(json.dumps({"ts": 1, "items": {}}), "utf-8")
    (tmp_path / "watchlist.tombstones.json").write_text("{broken", "utf-8")
    TrackerStore(tmp_path).read_state("history")
    assert "legacy_sidecar_skipped" in caplog.text
    assert (next(tmp_path.glob("json-backup-*")) / "watchlist.tombstones.json").is_file()
