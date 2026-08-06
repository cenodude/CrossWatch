from __future__ import annotations

import json
from typing import Any


class _Resp:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.ok = True
        self.status_code = 200
        self.headers = {"content-type": "application/json"}
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload

    @property
    def text(self) -> str:
        return json.dumps(self._payload)


MOVIE_ROWS = [
    {"ratingKey": "11", "guid": "plex://movie/aaa", "Guid": [{"id": "tmdb://603"}, {"id": "imdb://tt0133093"}]},
    {"ratingKey": "12", "guid": "plex://movie/bbb", "Guid": [{"id": "tmdb://604"}]},
]
SHOW_ROWS = [
    {"ratingKey": "21", "guid": "plex://show/ccc", "Guid": [{"id": "tvdb://81797"}]},
]


class _Section:
    def __init__(self, key: str, type_: str) -> None:
        self.key = key
        self.type = type_


class _Adapter:
    def __init__(self, sections) -> None:
        self._sections = sections
        self.client = type("C", (), {"server": _Server()})()

    def libraries(self, types=()):
        return self._sections


class _Session:
    headers: dict[str, str] = {}

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {})})
        sid = url.rsplit("/all", 1)[0].rsplit("/", 1)[-1]
        start = int((params or {}).get("X-Plex-Container-Start") or 0)
        rows = MOVIE_ROWS if sid == "1" else SHOW_ROWS
        page = rows[start:]
        return _Resp({"MediaContainer": {"Metadata": page, "totalSize": len(rows)}})


class _Server:
    _session = None
    _token = "TOK"

    def url(self, path):
        return f"http://pms{path}"


def _setup(monkeypatch):
    from providers.sync.plex import _history as h

    ses = _Session()
    srv = _Server()
    srv._session = ses
    adapter = _Adapter([_Section("1", "movie"), _Section("2", "show")])
    adapter.client = type("C", (), {"server": srv})()

    monkeypatch.setattr(h, "_as_base_url", lambda _s: "http://pms")
    monkeypatch.setattr(h, "_load_guid_index", lambda *a, **k: False)
    monkeypatch.setattr(h, "_save_guid_index", lambda *a, **k: None)
    h._clear_guid_index()
    h._GUID_INDEX_KEY = None
    return h, adapter, ses


def test_guid_index_is_built_from_paged_rows(monkeypatch) -> None:
    h, adapter, ses = _setup(monkeypatch)

    h._build_guid_index(adapter, set(), force=True)

    assert h._GUID_INDEX_MOVIE == {
        "plex://movie/aaa": "11",
        "tmdb://603": "11",
        "imdb://tt0133093": "11",
        "plex://movie/bbb": "12",
        "tmdb://604": "12",
    }
    assert h._GUID_INDEX_SHOW == {"plex://show/ccc": "21", "tvdb://81797": "21"}


def test_requests_ask_for_guids_and_correct_types(monkeypatch) -> None:
    h, adapter, ses = _setup(monkeypatch)

    h._build_guid_index(adapter, set(), force=True)

    assert all(c["params"].get("includeGuids") == 1 for c in ses.calls)
    types = {c["url"].rsplit("/all", 1)[0].rsplit("/", 1)[-1]: c["params"]["type"] for c in ses.calls}
    assert types["1"] == 1, "movie sections must query type=1"
    assert types["2"] == 2, "show sections must query type=2 (show, not episode)"


def test_one_request_per_page_not_per_item(monkeypatch) -> None:
    h, adapter, ses = _setup(monkeypatch)

    h._build_guid_index(adapter, set(), force=True)

    assert len(ses.calls) == 2, f"expected one request per section, got {len(ses.calls)}"


def test_allow_filter_skips_other_sections(monkeypatch) -> None:
    h, adapter, ses = _setup(monkeypatch)

    h._build_guid_index(adapter, {"1"}, force=True)

    assert len(ses.calls) == 1
    assert h._GUID_INDEX_SHOW == {}


def test_row_guids_reads_attribute_and_children() -> None:
    from providers.sync.plex._history import _row_guids

    assert _row_guids(MOVIE_ROWS[0]) == ["plex://movie/aaa", "tmdb://603", "imdb://tt0133093"]
    assert _row_guids({"ratingKey": "9"}) == []
