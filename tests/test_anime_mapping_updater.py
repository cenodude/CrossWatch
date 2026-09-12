import json
from types import SimpleNamespace

import pytest

from cw_platform.anime_mapping import storage, updater


GENERATED = "2026-09-06T06:44:44Z"
MAPPINGS = {"mal:1": {"anilist:1": {"1-12": "1-12"}}}


def identity(simkl):
    return f"title\tanidb\tanilist\tkitsu\tmyanimelist\tsimkl\nAnime\t1\t1\t10\t1\t{simkl}\n".encode()


class Response:
    def __init__(self, body=b"", status=200, headers=None):
        self.body, self.status_code, self.headers = body, status, headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("Download failed")

    def json(self):
        return json.loads(self.body)

    def iter_content(self, size):
        yield self.body


@pytest.fixture
def installed(config_base, monkeypatch):
    pp = storage.paths("v3")
    pp["root"].mkdir(parents=True)
    pp["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    pp["identity"].write_bytes(identity(100))
    storage.rebuild_sqlite_from_mappings()
    storage.write_state("v3", {
        "dataset_generated_on": GENERATED, "last_updated_at": 10,
        "indexed_identity_sha256": updater._file_sha256(pp["identity"]),
        "indexed_mappings_sha256": updater._file_sha256(pp["mappings"]),
        "identity_etag": '"old"',
    })
    remote = SimpleNamespace(generated=GENERATED, stats_error=False,
                             release=Response(b"[]"),
                             identity=Response(identity(100), headers={"ETag": '"old"'}),
                             mappings=Response(json.dumps(MAPPINGS).encode()), calls=[], logs=[])
    def get(url, **kwargs):
        remote.calls.append((url, kwargs.get("headers", {})))
        if url == updater.IDENTITY_COMMITS_URL:
            return remote.release
        if url.endswith("stats.json"):
            if remote.stats_error:
                raise RuntimeError("Stats unavailable")
            return Response(json.dumps({"meta": {"generated_on": remote.generated}}).encode())
        return remote.identity if url.endswith("/database/animeapi.tsv") else remote.mappings
    monkeypatch.setattr(updater.requests, "get", get)
    monkeypatch.setattr(updater, "log", lambda message, **kwargs: remote.logs.append(message))
    return pp, remote


def test_identity_updates_without_anibridge_change(installed):
    pp, remote = installed
    remote.identity = Response(identity(200), headers={"ETag": '"new"'})
    result = updater.update()
    assert result["ok"] and result["updated"] and result["identity_updated"]
    assert not result["mappings_updated"]
    assert storage.query_native_identity("v3", "mal", "1")["simkl"] == "200"
    assert all(not url.endswith("mappings.min.json") for url, _ in remote.calls)
    state = storage.read_state("v3")
    assert state["identity_etag"] == '"new"'
    assert state["dataset_generated_on"] == GENERATED


@pytest.mark.parametrize("http_status", [200, 304])
def test_unchanged_identity_does_not_rebuild(installed, monkeypatch, http_status):
    _, remote = installed
    remote.identity = Response(identity(100), status=http_status)
    monkeypatch.setattr(updater, "rebuild_sqlite_from_mappings", lambda **kwargs: pytest.fail("Unnecessary rebuild"))
    result = updater.update()
    assert result["ok"] and not result["updated"] and not result["rebuilt"]
    assert remote.calls[-1][1]["If-None-Match"] == '"old"'
    assert storage.read_state("v3")["last_updated_at"] == 10
    assert "update_skipped reason=datasets_current" in remote.logs
    assert not list(storage.paths()["root"].glob(".*.tmp"))


def test_missing_identity_is_downloaded_without_conditional_headers(installed):
    pp, remote = installed
    pp["identity"].unlink()
    # Model a previous installation that never obtained identity data.
    storage.write_state("v3", {"indexed_identity_sha256": "", "identity_error": "RuntimeError"})
    result = updater.update()
    assert result["identity_updated"] and not result["identity_error"]
    assert "If-None-Match" not in remote.calls[-1][1]
    assert pp["identity"].exists()


@pytest.mark.parametrize("invalid", [False, True])
def test_identity_failure_keeps_cached_data_and_is_retried(installed, invalid):
    pp, remote = installed
    before = pp["identity"].read_bytes()
    remote.identity = Response(b"invalid identity data", status=200 if invalid else 500)
    result = updater.update()
    assert not result["ok"] and "animeApi update failed" in result["error"]
    assert pp["identity"].read_bytes() == before
    assert storage.read_state("v3")["identity_etag"] == '"old"'
    assert not any("datasets_current" in message for message in remote.logs)
    remote.identity = Response(identity(200), headers={"etag": '"new"'})
    result = updater.update()
    assert result["ok"] and result["identity_updated"] and not result["error"]


def test_anibridge_failure_does_not_block_identity_update(installed):
    _, remote = installed
    remote.stats_error = True
    remote.identity = Response(identity(200))
    result = updater.update()
    assert not result["ok"] and result["identity_updated"]
    assert "aniBridge update failed" in result["error"]
    assert storage.query_native_identity("v3", "mal", "1")["simkl"] == "200"
    assert storage.read_state("v3")["dataset_generated_on"] == GENERATED


def test_failed_mapping_download_does_not_advance_installed_version(installed):
    _, remote = installed
    remote.generated = "2026-09-12T00:00:00Z"
    remote.mappings = Response(status=500)
    assert not updater.update()["ok"]
    assert storage.read_state("v3")["dataset_generated_on"] == GENERATED
    remote.mappings = Response(json.dumps({"mal:2": {"anilist:2": {"1": "1"}}}).encode())
    assert updater.update()["mappings_updated"]
    assert sum(url.endswith("mappings.min.json") for url, _ in remote.calls) == 2


def test_failed_rebuild_is_retried_with_unchanged_download(installed, monkeypatch):
    pp, remote = installed
    old_hash = storage.read_state("v3")["indexed_identity_sha256"]
    remote.identity = Response(identity(200))
    real_rebuild = updater.rebuild_sqlite_from_mappings
    def fail(**kwargs):
        raise RuntimeError("Index unavailable")
    monkeypatch.setattr(updater, "rebuild_sqlite_from_mappings", fail)
    with pytest.raises(RuntimeError):
        updater.update()
    assert storage.read_state("v3")["indexed_identity_sha256"] == old_hash
    assert pp["identity"].read_bytes() == identity(200)
    remote.identity = Response(status=304)
    monkeypatch.setattr(updater, "rebuild_sqlite_from_mappings", real_rebuild)
    result = updater.update()
    assert result["ok"] and result["identity_updated"] and not result["error"]
    assert storage.query_native_identity("v3", "mal", "1")["simkl"] == "200"


def test_force_downloads_both_datasets_and_rebuilds(installed):
    _, remote = installed
    result = updater.update(force=True)
    assert result["ok"] and result["rebuilt"]
    assert any(url.endswith("mappings.min.json") for url, _ in remote.calls)
    assert "If-None-Match" not in remote.calls[-1][1]


@pytest.mark.parametrize("missing_db", [False, True])
def test_schema_or_missing_index_rebuilds_unchanged_datasets(installed, missing_db):
    pp, remote = installed
    remote.identity = Response(status=304)
    if missing_db:
        pp["db"].unlink()
    else:
        storage.write_state("v3", {"schema_version": 0})
    result = updater.update()
    assert result["ok"] and result["rebuilt"] and not result["updated"]
    assert storage.index_ready()


def test_missing_file_cannot_accept_http_not_modified(installed):
    pp, remote = installed
    pp["identity"].unlink()
    remote.identity = Response(status=304)
    result = updater.update()
    assert not result["ok"] and not pp["identity"].exists()


def test_first_install_downloads_and_indexes_both_sources(installed):
    pp, remote = installed
    for name in ("mappings", "identity", "db", "state"):
        pp[name].unlink()
    result = updater.update()
    assert result["ok"] and result["mappings_updated"] and result["identity_updated"]
    assert storage.index_ready()
    assert storage.query_native_identity("v3", "mal", "1")["simkl"] == "100"


def test_last_modified_is_used_without_an_etag(installed):
    _, remote = installed
    storage.write_state("v3", {"identity_etag": "", "identity_last_modified": "Sun, 06 Sep 2026 09:14:06 GMT"})
    remote.identity = Response(status=304)
    assert updater.update()["ok"]
    assert remote.calls[-1][1]["If-Modified-Since"] == "Sun, 06 Sep 2026 09:14:06 GMT"


def test_auto_update_retries_identity_failure_before_normal_interval(installed, monkeypatch):
    from cw_platform.anime_mapping import auto_update
    _, remote = installed
    remote.identity = Response(status=500)
    assert not updater.update()["ok"]
    checked = storage.read_state("v3")["last_checked_at"]
    monkeypatch.setattr(auto_update.time, "time", lambda: checked + auto_update.MAX_SLEEP_SECONDS)
    worker = auto_update.AnimeMappingAutoUpdater(lambda: {"anime_mapping": {"enabled": True, "refresh_hours": 24}})
    monkeypatch.setattr(worker, "_sleep", lambda seconds: worker._stop.set())
    before = len(remote.calls)
    worker._loop()
    assert len(remote.calls) > before
    assert "animeApi update failed" in worker.status()["last_error"]
    assert worker.status()["next_check_at"] == checked + 2 * auto_update.MAX_SLEEP_SECONDS


def test_identity_publication_matches_the_downloaded_revision(installed):
    _, remote = installed
    revision = "a" * 40
    published = "2026-09-11T09:25:19Z"
    remote.release = Response(json.dumps([{"sha": revision, "commit": {"committer": {"date": published}}}]).encode())
    remote.identity = Response(identity(200))
    result = updater.update()
    assert result["identity_revision"] == revision
    assert result["identity_generated_on"] == published
    assert remote.calls[-1][0] == updater.IDENTITY_URL.replace("/v3/", f"/{revision}/")
    remote.identity = Response(status=304)
    again = updater.update()
    assert not again["updated"]
    assert again["identity_generated_on"] == published


def test_missing_release_metadata_never_labels_new_bytes_with_an_old_date(installed):
    _, remote = installed
    storage.write_state("v3", {"identity_revision": "a" * 40, "identity_generated_on": GENERATED})
    remote.release = Response(status=403)
    remote.identity = Response(identity(200))
    result = updater.update()
    assert result["ok"] and result["identity_updated"]
    assert not result["identity_generated_on"] and not result["identity_revision"]


def test_failed_identity_download_preserves_installed_revision(installed):
    _, remote = installed
    storage.write_state("v3", {"identity_revision": "a" * 40, "identity_generated_on": GENERATED})
    remote.release = Response(json.dumps([{"sha": "b" * 40, "commit": {"committer": {"date": "2026-09-11T09:25:19Z"}}}]).encode())
    remote.identity = Response(status=500)
    result = updater.update()
    assert not result["ok"]
    assert result["identity_generated_on"] == GENERATED
    assert result["identity_revision"] == "a" * 40
