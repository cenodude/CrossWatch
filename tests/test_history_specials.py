from __future__ import annotations

from types import SimpleNamespace

from cw_platform.id_map import canonical_key, minimal
from cw_platform.orchestrator import _applier, _unresolved
from providers.sync.mdblist._history import _bucketize
from providers.sync.publicmetadb._history import _payload_for_item, _to_minimal
from providers.sync._mod_SIMKL import _confirmed_keys
from providers.sync.simkl import _history as simkl_history
from providers.sync.simkl._common import key_of as simkl_key_of
from providers.sync.trakt._history import _batch_add, _batch_remove


WATCHED_AT = "2026-01-02T03:04:05Z"
SHOW_IDS = {"tmdb": "63372", "tvdb": "295685", "trakt": "100"}
ISSUE_311_ITEM = {
    "type": "episode",
    "title": "The Making of The Walking Dead",
    "season": 0,
    "episode": 48,
    "watched_at": "2023-10-12T12:28:00.000Z",
    "ids": {"tmdb": "63372", "tvdb": "2960601", "trakt": "5942444"},
    "show_ids": {
        "tmdb": "1402",
        "imdb": "tt1520211",
        "tvdb": "153021",
        "trakt": "1393",
        "slug": "the-walking-dead",
    },
}


def _special(number: int) -> dict[str, object]:
    return {
        "type": "episode",
        "ids": dict(SHOW_IDS),
        "show_ids": dict(SHOW_IDS),
        "season": 0,
        "episode": number,
        "watched_at": WATCHED_AT,
    }


def test_special_episode_canonical_key_keeps_season_zero() -> None:
    item = {"type": "episode", "show_ids": {"tmdb": "63372"}, "season": 0, "episode": 1}

    assert canonical_key(item) == "tmdb:63372#s00e01"


def test_publicmetadb_special_episode_round_trip_shape() -> None:
    row = {
        "id": "history-1",
        "tmdb_id": 63372,
        "media_type": "tv",
        "season": 0,
        "episode": 1,
        "watched_at": WATCHED_AT,
    }

    parsed = _to_minimal(row)
    payload, hint = _payload_for_item(_special(1))

    assert parsed is not None
    assert parsed["season"] == 0
    assert parsed["episode"] == 1
    assert parsed["show_ids"] == {"tmdb": "63372"}
    assert payload == {
        "tmdb_id": 63372,
        "media_type": "tv",
        "season": 0,
        "episode": 1,
        "watched_at": WATCHED_AT,
    }
    assert hint is None


def test_mdblist_groups_multiple_specials_under_one_season() -> None:
    body, accepted = _bucketize([_special(1), _special(2)], unwatch=False)

    shows = body["shows_nested"]
    assert len(shows) == 1
    assert shows[0]["seasons"] == [
        {
            "number": 0,
            "episodes": [
                {"number": 1, "watched_at": WATCHED_AT},
                {"number": 2, "watched_at": WATCHED_AT},
            ],
        }
    ]
    assert len(accepted) == 2


def test_trakt_special_episode_add_and_remove_shapes() -> None:
    adapter = SimpleNamespace(config={})

    add_body, add_unresolved, *_ = _batch_add(adapter, [_special(1), _special(2)])
    remove_body, remove_unresolved, *_ = _batch_remove(adapter, [_special(1), _special(2)])

    add_season = add_body["shows"][0]["seasons"][0]
    remove_season = remove_body["shows"][0]["seasons"][0]
    assert add_season["number"] == 0
    assert [episode["number"] for episode in add_season["episodes"]] == [1, 2]
    assert remove_season == {"number": 0, "episodes": [{"number": 1}, {"number": 2}]}
    assert add_unresolved == []
    assert remove_unresolved == []


def test_simkl_special_episode_is_available_as_history_source() -> None:
    row = {
        "show": {
            "title": "Example Show",
            "year": 2020,
            "ids": dict(SHOW_IDS),
        },
        "seasons": [
            {
                "number": 0,
                "episodes": [
                    {
                        "number": 1,
                        "watched_at": WATCHED_AT,
                        "ids": {"tvdb": "900001"},
                    },
                    {"number": 2, "watched_at": WATCHED_AT},
                ],
            }
        ],
    }

    items, _thaw, _movies_ts, shows_ts, _anime_ts, movies_count, episode_count = simkl_history._parse_rows(
        [], [row], [], limit=None
    )

    assert len(items) == 2
    by_episode = {item["episode"]: item for item in items.values()}
    assert by_episode[1]["season"] == 0
    assert by_episode[1]["ids"] == {"tvdb": "900001"}
    assert by_episode[2]["season"] == 0
    assert by_episode[2]["ids"] == SHOW_IDS
    assert by_episode[2]["show_ids"] == SHOW_IDS
    assert shows_ts is not None
    assert movies_count == 0
    assert episode_count == 2


def test_simkl_special_episode_remove_uses_episode_lookup_ids(monkeypatch) -> None:
    requests: list[dict[str, object]] = []

    class Session:
        def post(self, _url, **kwargs):
            requests.append(kwargs["json"])
            return SimpleNamespace(status_code=200, text="", json=lambda: {})

    monkeypatch.setattr(simkl_history, "_unfreeze", lambda _keys: None)
    monkeypatch.setattr(simkl_history, "_evict_removes_from_cache", lambda _items: None)
    adapter = SimpleNamespace(
        client=SimpleNamespace(session=Session()),
        cfg=SimpleNamespace(timeout=5, history_chunk_size=100, api_key="key", access_token="token"),
    )
    item = _special(1)
    item["ids"] = {"tvdb": "900001"}

    applied, unresolved = simkl_history.remove(adapter, [item])

    assert applied == 1
    assert unresolved == []
    episode = requests[0]["shows"][0]["seasons"][0]["episodes"][0]
    assert episode == {"number": 1, "ids": {"tvdb": "900001"}}


def test_issue_311_simkl_add_sends_episode_lookup_id(monkeypatch) -> None:
    requests: list[dict[str, object]] = []

    class Session:
        def post(self, _url, **kwargs):
            requests.append(kwargs["json"])
            payload = {
                "added": {"movies": 0, "shows": 0, "episodes": 1},
                "not_found": {"movies": [], "shows": [], "episodes": []},
            }
            return SimpleNamespace(status_code=201, text="json", json=lambda: payload)

    monkeypatch.setattr(simkl_history, "_unfreeze", lambda _keys: None)
    monkeypatch.setattr(simkl_history, "_inject_adds_into_cache", lambda _items: None)
    adapter = SimpleNamespace(
        client=SimpleNamespace(session=Session()),
        cfg=SimpleNamespace(timeout=5, api_key="key", access_token="token"),
    )

    applied, unresolved = simkl_history.add(adapter, [dict(ISSUE_311_ITEM)])

    assert applied == 1
    assert unresolved == []
    show = requests[0]["shows"][0]
    assert show["ids"]["tmdb"] == "1402"
    assert show["seasons"] == [
        {
            "number": 0,
            "episodes": [
                {
                    "number": 48,
                    "watched_at": "2023-10-12T12:28:00.000Z",
                    "ids": {"tvdb": "2960601"},
                }
            ],
        }
    ]


def test_issue_311_rejected_special_is_blocked_after_first_attempt(monkeypatch, tmp_path) -> None:
    class Session:
        def post(self, _url, **kwargs):
            payload = {
                "added": {"movies": 0, "shows": 0, "episodes": 0},
                "not_found": {"movies": [], "shows": [], "episodes": kwargs["json"]["shows"]},
            }
            return SimpleNamespace(status_code=201, text="json", json=lambda: payload)

    monkeypatch.setattr(simkl_history, "_unfreeze", lambda _keys: None)
    monkeypatch.setattr(_unresolved, "STATE_DIR", tmp_path)
    monkeypatch.setenv("CW_PAIR_KEY", "TRAKT-SIMKL-history-issue311")
    adapter = SimpleNamespace(
        client=SimpleNamespace(session=Session()),
        cfg=SimpleNamespace(timeout=5, api_key="key", access_token="token"),
    )
    source_item = dict(ISSUE_311_ITEM)

    applied, unresolved = simkl_history.add(adapter, [source_item])
    confirmed = _confirmed_keys(simkl_key_of, [source_item], unresolved)
    normalized = _applier._normalize(
        {"ok": True, "count": applied, "unresolved": unresolved, "confirmed_keys": confirmed},
        [minimal(source_item)],
        "add",
        dst="SIMKL",
        feature="history",
        emit=lambda *_args, **_kwargs: None,
    )
    blocked = _unresolved.load_unresolved_keys("SIMKL", "history", cross_features=True)

    expected_key = canonical_key(minimal(source_item))
    assert applied == 0
    assert normalized["unresolved"] == 1
    assert confirmed == []
    assert blocked == {expected_key}
