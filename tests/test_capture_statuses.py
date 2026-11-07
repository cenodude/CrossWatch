# tests/test_capture_statuses.py
# CrossWatch - Native status capture, restore and pending-write protection.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from cw_platform.orchestrator import _status_mapping as runtime
from cw_platform.status_mapping import Capabilities, Observation, ReadState, Status
from cw_platform.status_mapping_store import StatusStore
from providers.sync._status_mapping import NativeStatusAdapter, Record
from services import capture_statuses as service, snapshots


def record(status=Status.DROPPED, ident='1'):
    return Record(ident, 'shows', {'type': 'show', 'ids': {'tmdb': ident}, 'title': 'A show', 'secret': 'omit'},
                  Observation(ReadState.PRESENT, status, status.value, 'protected', False, False), {'secret': 'omit'})


@pytest.fixture
def case(monkeypatch, tmp_path, config_base):
    state = SimpleNamespace(rows={'1': record()}, reads=[], writes=[], features=[], fault=None, alter=lambda: None,
                            feature_rows={}, mutations=[], operation_result={'ok': True, 'count': 1})
    class Adapter(NativeStatusAdapter):
        capabilities = {'shows': Capabilities(frozenset(Status), frozenset({Status.WATCHING, Status.ON_HOLD, Status.DROPPED}))}

        def read(self):
            state.reads.append(deepcopy(self.cfg))
            state.alter()
            if state.fault == 'read':
                raise TimeoutError('failed read')
            return deepcopy(state.rows)

        def write(self, row, desired):
            state.writes.append((row.native_id, desired))
            if state.fault == 'timeout':
                raise TimeoutError('lost response')
            state.rows[row.native_id] = replace(row, observation=replace(row.observation, status=desired, native_status=desired.value))
            if state.fault == 'applied_timeout':
                raise TimeoutError('lost response after application')
            if state.fault == 'readback':
                state.rows[row.native_id] = replace(row, observation=Observation(ReadState.UNKNOWN))

    class Ops:
        def is_configured(self, cfg):
            return True

        def features(self):
            return {'history': True, 'progress': True}

        def build_index(self, cfg, *, feature):
            state.features.append(feature)
            return deepcopy(state.feature_rows.get(feature, {}))

        def add(self, cfg, items, *, feature, dry_run=False):
            state.mutations.append(('add', feature, deepcopy(items)))
            return deepcopy(state.operation_result)

        def remove(self, cfg, items, *, feature, dry_run=False):
            state.mutations.append(('remove', feature, deepcopy(items)))
            return deepcopy(state.operation_result)

    monkeypatch.setattr(service, 'adapter_class', lambda _: Adapter)
    monkeypatch.setattr(runtime, 'adapter_class', lambda _: Adapter)
    monkeypatch.setattr(snapshots, 'CONFIG', tmp_path)
    monkeypatch.setattr(snapshots, 'load_sync_ops', lambda _: Ops())
    state.store = StatusStore(tmp_path / 'statuses.sqlite3')
    monkeypatch.setattr(runtime, 'StatusStore', lambda: state.store)
    monkeypatch.setattr(service, 'StatusStore', lambda: state.store)
    state.cfg = {'version': 'test', 'simkl': {'instances': {'P01': {'label': 'Second'}}}}
    state.payload = service.capture_statuses(state.cfg, 'SIMKL', 'default')
    state.reads.clear()
    return state


def restore(case, payload=None):
    return service.restore_statuses(case.cfg, 'SIMKL', 'P01', payload or case.payload, store=case.store)


def test_capture_keeps_native_status_without_provider_secrets(case):
    row = case.payload['items'][0]
    assert row['observation']['status'] == 'dropped'
    assert row['observation']['native_status'] == 'dropped'
    assert row['item']['ids'] == {'tmdb': '1'}
    assert 'secret' not in str(case.payload)


@pytest.mark.parametrize('feature', ['history', 'all'])
def test_empty_history_still_captures_note_statuses(case, feature):
    created = snapshots.create_snapshot('SIMKL', feature, cfg=case.cfg)
    assert created['ok'] and created['stats']['title_statuses'] == 1
    path = created['path'] if feature == 'history' else created['children'][0]['path']
    saved = snapshots.read_snapshot(path)
    assert saved['items'] == {}
    assert saved['title_statuses'] == case.payload


@pytest.mark.parametrize('feature', ['history', 'all'])
def test_capture_failure_does_not_claim_success(case, feature):
    case.fault = 'read'
    if feature == 'history':
        with pytest.raises(TimeoutError):
            snapshots.create_snapshot('SIMKL', feature, cfg=case.cfg)
    else:
        created = snapshots.create_snapshot('SIMKL', feature, cfg=case.cfg)
        assert not created['ok']
        assert created['children'][0]['error']


def test_restore_reads_live_destination_only_and_is_repeatable(case):
    case.rows['1'] = record(Status.WATCHING)
    result = restore(case)
    assert result['ok'] and result['updated'] == 1
    assert len(case.reads) == 3
    assert all(cfg['_cw_provider_instance'] == 'P01' for cfg in case.reads)
    assert restore(case)['unchanged'] == 1
    assert len(case.writes) == 1
    case.rows['1'] = record(Status.ON_HOLD)
    assert restore(case)['updated'] == 1


@pytest.mark.parametrize('fault', ['timeout', 'readback'])
def test_uncertainty_survives_restart_and_another_capture(case, fault):
    case.rows['1'] = record(Status.WATCHING)
    case.fault = fault
    assert not restore(case)['ok']
    assert len(case.writes) == 1
    case.store = StatusStore(case.store.path)
    pending = case.store.pending_targets()
    assert pending
    case.fault = None
    case.rows['1'] = record(Status.WATCHING)
    assert not restore(case)['ok']
    other = deepcopy(case.payload)
    other['items'][0]['observation']['status'] = 'on_hold'
    assert not restore(case, other)['ok']
    assert len(case.writes) == 1
    assert case.store.pending_targets() == pending
    case.rows['1'] = record(Status.DROPPED)
    assert restore(case)['recovered'] == 1
    assert not case.store.pending_targets()


@pytest.mark.parametrize('guard', ['rewatching', 'unknown', 'changed', 'completed'])
def test_restore_preserves_protected_destination(case, guard):
    case.rows['1'] = record(Status.WATCHING)
    if guard == 'rewatching':
        case.rows['1'].observation = replace(case.rows['1'].observation, rewatching=True)
    elif guard == 'unknown':
        case.rows['1'].observation = Observation(ReadState.UNKNOWN)
    elif guard == 'completed':
        case.rows['1'] = record(Status.COMPLETED)
    else:
        def alter():
            if len(case.reads) == 2:
                case.rows['1'] = record(Status.ON_HOLD)
        case.alter = alter
    assert not restore(case)['ok']
    assert not case.writes


def test_planned_and_unsupported_statuses_are_captured_but_not_written(case):
    case.rows = {'1': record(Status.PLANNED), '2': record(Status.COMPLETED, '2')}
    payload = service.capture_statuses(case.cfg, 'SIMKL', 'default')
    case.rows = {key: record(Status.WATCHING, key) for key in case.rows}
    result = restore(case, payload)
    assert result['ok'] and result['excluded'] == 2
    assert not case.writes


def test_incomplete_captured_observation_never_authorizes_restore(case):
    case.payload['items'][0]['observation']['rewatching'] = None
    case.rows['1'] = record(Status.WATCHING)
    result = restore(case)
    assert not result['ok'] and not case.writes
    assert any('incomplete observation' in error for error in result['errors'])


def test_full_restore_applies_statuses_after_regular_features(case):
    created = snapshots.create_snapshot('SIMKL', 'all', cfg=case.cfg)
    case.rows['1'] = record(Status.WATCHING)
    case.features.clear()
    def check_order():
        assert case.features == ['history', 'progress']
    case.alter = check_order
    result = snapshots.restore_snapshot(created['path'], instance_id='P01', cfg=case.cfg)
    assert result['ok'] and result['title_statuses']['updated'] == 1
    assert '1 restored' in result['message']


def test_old_capture_restores_without_reading_statuses(case, tmp_path):
    import json
    path = tmp_path / 'snapshots' / 'old.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'provider': 'SIMKL', 'feature': 'history', 'items': {}}))
    result = snapshots.restore_snapshot('old.json', cfg=case.cfg)
    assert result['ok'] and 'title_statuses' not in result
    assert not case.reads and not case.writes


@pytest.mark.parametrize('extended', [False, True])
def test_comparison_includes_status_only_changes(case, extended):
    first = snapshots.create_snapshot('SIMKL', 'history', cfg=case.cfg, label='before')
    case.rows['1'] = record(Status.ON_HOLD)
    second = snapshots.create_snapshot('SIMKL', 'history', cfg=case.cfg, label='after')
    compare = snapshots.diff_snapshots_extended if extended else snapshots.diff_snapshots
    result = compare(first['path'], second['path'])
    assert result['summary']['updated'] == 1
    row = (result['items'] if extended else result['updated'])[0]
    assert row['old']['status'] == 'dropped' and row['new']['status'] == 'on_hold'
    assert row['new']['record_type'] == 'title_status'
    assert 'protected_token' not in str(row)


def test_cancel_during_status_preflight_never_writes_and_finishes_progress(case):
    from cw_platform.run_control import SyncCancelled, clear_cancel, request_cancel
    capture = snapshots.create_snapshot('SIMKL', 'history', cfg=case.cfg)
    case.reads.clear()
    case.rows['1'] = record(Status.WATCHING)
    def cancel():
        if len(case.reads) == 2:
            request_cancel()
    case.alter = cancel
    try:
        with pytest.raises(SyncCancelled):
            snapshots.restore_snapshot(capture['path'], cfg=case.cfg, progress_id='cancel-status-restore')
        assert not case.writes
        progress = snapshots.get_capture_progress('cancel-status-restore')
        assert progress['done'] and not progress['ok']
    finally:
        clear_cancel()


def test_invalid_status_payload_fails_before_feature_restore(case, tmp_path):
    import json
    capture = snapshots.create_snapshot('SIMKL', 'history', cfg=case.cfg)
    path = tmp_path / 'snapshots' / capture['path']
    payload = json.loads(path.read_text())
    payload['title_statuses']['version'] = 99
    path.write_text(json.dumps(payload))
    case.features.clear()
    with pytest.raises(ValueError, match='Invalid captured'):
        snapshots.restore_snapshot(capture['path'], cfg=case.cfg)
    assert not case.features and not case.writes


@pytest.mark.parametrize('existing', [True, False])
def test_real_crosswatch_note_roundtrip(tmp_path, config_base, existing):
    from cw_platform.tracker_store import TrackerStore
    source, target = TrackerStore(tmp_path / 'source'), TrackerStore(tmp_path / 'target')
    source.write_state('history', {})
    target.write_state('history', {})
    key = 'show:tmdb:1'
    source.save_note(key, {'type': 'show', 'tmdb': '1', 'title': 'A show', 'status': 'on_hold'})
    if existing:
        target.save_note(key, {'type': 'show', 'tmdb': '1', 'title': 'A show', 'status': 'watching', 'notes': 'Keep'})
    cfg = {'crosswatch': {'root_dir': str(tmp_path / 'source'), 'instances': {'P01': {'root_dir': str(tmp_path / 'target')}}}}
    payload = service.capture_statuses(cfg, 'CROSSWATCH', 'default')
    result = service.restore_statuses(cfg, 'CROSSWATCH', 'P01', payload, store=StatusStore(tmp_path / 'status.sqlite3'))
    assert result['ok'] and result['updated'] == 1, result
    assert target.note(key)['status'] == 'on_hold'
    if existing:
        assert target.note(key)['notes'] == 'Keep'
    assert target.read_current('history')['items'] == {}
    assert not target.has_state('watchlist')


def test_real_crosswatch_history_capture_restores_note_only_title(tmp_path, config_base, monkeypatch):
    from cw_platform.tracker_store import TrackerStore
    store = TrackerStore(tmp_path / 'tracker')
    store.write_state('history', {})
    key = 'show:tmdb:42'
    note = {'type': 'show', 'tmdb': '42', 'title': 'A show', 'status': 'dropped', 'notes': 'Keep'}
    store.save_note(key, note)
    monkeypatch.setattr(snapshots, 'CONFIG', tmp_path)
    cfg = {'crosswatch': {'connected': True, 'root_dir': str(tmp_path / 'tracker')}}
    capture = snapshots.create_snapshot('CROSSWATCH', 'history', cfg=cfg)
    assert capture['stats']['count'] == 0 and capture['stats']['title_statuses'] == 1
    store.save_note(key, {**note, 'status': 'watching'})
    restored = snapshots.restore_snapshot(capture['path'], cfg=cfg)
    assert restored['ok'] and restored['title_statuses']['updated'] == 1, restored
    assert store.note(key)['status'] == 'dropped'
    assert store.note(key)['notes'] == 'Keep'
    assert store.read_current('history')['items'] == {}


def feature_capture(case, tmp_path, *, statuses=True, feature='history'):
    import json
    row = {'type': 'movie', 'ids': {'tmdb': '1'}, 'title': 'Movie'}
    payload = {'provider': 'SIMKL', 'instance': 'default', 'feature': feature, 'items': {'movie:1': row}}
    if statuses:
        payload['title_statuses'] = case.payload
    path = tmp_path / 'snapshots' / 'restore.json'
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(payload))
    return 'restore.json', row


@pytest.mark.parametrize('mode', ['merge', 'clear_restore'])
@pytest.mark.parametrize('statuses', [True, False])
def test_pending_blocks_regular_feature_writes_even_for_legacy_captures(case, tmp_path, mode, statuses):
    case.rows['1'] = record(Status.WATCHING)
    case.fault = 'applied_timeout'
    assert not restore(case)['ok']
    assert case.rows['1'].observation.status == Status.DROPPED
    case.store = StatusStore(case.store.path)
    before = case.store.pending_targets()
    path, row = feature_capture(case, tmp_path, statuses=statuses)
    if mode == 'clear_restore':
        case.feature_rows['history'] = {'movie:1': row}
    result = snapshots.restore_snapshot(path, cfg=case.cfg, instance_id='P01', mode=mode)
    assert not result['ok'] and any('still pending' in error for error in result['errors'])
    assert not case.mutations and len(case.writes) == 1
    assert case.store.pending_targets() == before
    assert case.rows['1'].observation.status == Status.DROPPED


def test_pending_other_instance_does_not_block_feature_restore(case, tmp_path):
    case.rows['1'] = record(Status.WATCHING)
    case.fault = 'timeout'
    assert not restore(case)['ok']
    path, _ = feature_capture(case, tmp_path, statuses=False)
    assert snapshots.restore_snapshot(path, cfg=case.cfg)['ok']
    assert len(case.mutations) == 1
    assert case.store.pending_targets()


@pytest.mark.parametrize('feature', ['history', 'all'])
@pytest.mark.parametrize('unknown', [True, False])
def test_incomplete_observations_fail_capture_instead_of_counting_missing_status(case, tmp_path, feature, unknown):
    case.rows['1'].observation = Observation(ReadState.UNKNOWN) if unknown else replace(case.rows['1'].observation, rewatching=None)
    if feature == 'history':
        with pytest.raises(ValueError, match='1 incomplete observations'):
            snapshots.create_snapshot('SIMKL', feature, cfg=case.cfg)
    else:
        result = snapshots.create_snapshot('SIMKL', feature, cfg=case.cfg)
        assert not result['ok']
        assert 'title_statuses' not in result['stats']
        assert '1 incomplete observations' in result['children'][0]['error']
    assert not list((tmp_path / 'snapshots').rglob('*__history__*.json'))


@pytest.mark.parametrize('extended', [True, False])
@pytest.mark.parametrize('field', ['rewatching', 'watchlisted'])
def test_compare_exposes_preservation_flag_changes(case, extended, field):
    before = snapshots.create_snapshot('SIMKL', 'history', cfg=case.cfg, label='before-flags')
    case.rows['1'].observation = replace(case.rows['1'].observation, **{field: True})
    after = snapshots.create_snapshot('SIMKL', 'history', cfg=case.cfg, label='after-flags')
    compare = snapshots.diff_snapshots_extended if extended else snapshots.diff_snapshots
    result = compare(before['path'], after['path'])
    assert result['summary']['updated'] == 1
    row = (result['items'] if extended else result['updated'])[0]
    assert row['old'][field] is False and row['new'][field] is True
    assert {'path': field, 'old': False, 'new': True} in row['changes']


@pytest.mark.parametrize('operation_result', [
    {'ok': False, 'count': 0, 'unresolved': [{'reason': 'failed'}]},
    {'ok': False, 'count': 1},
    {'ok': True, 'count': 0, 'errors': 1},
    {'ok': True, 'count': 0, 'unresolved': [{'reason': 'failed'}]},
    {'ok': True, 'count': 0, 'accepted_unverified': 1},
    {'ok': True}, None,
])
@pytest.mark.parametrize('mode', ['merge', 'clear_restore'])
def test_unsuccessful_regular_result_never_allows_status_stage(case, tmp_path, operation_result, mode):
    path, row = feature_capture(case, tmp_path)
    case.rows['1'] = record(Status.WATCHING)
    case.operation_result = operation_result
    if mode == 'clear_restore':
        case.feature_rows['history'] = {'movie:1': row}
    result = snapshots.restore_snapshot(path, cfg=case.cfg, mode=mode)
    assert not result['ok'] and result['errors']
    assert not case.writes and 'title_statuses' not in result
    assert len(case.mutations) == 1
    assert result['added' if mode == 'merge' else 'removed'] == (operation_result or {}).get('count', 0)


def test_successful_zero_count_is_not_replaced_with_batch_length(case, tmp_path):
    path, _ = feature_capture(case, tmp_path, statuses=False)
    case.operation_result = {'ok': True, 'count': 0}
    result = snapshots.restore_snapshot(path, cfg=case.cfg)
    assert result['ok'] and result['added'] == 0


@pytest.fixture
def mal_case(monkeypatch, tmp_path, config_base):
    from providers.sync.myanimelist._status import StatusAdapter
    from providers.sync.myanimelist import _history
    entry = {'status': 'dropped', 'score': 0, 'num_episodes_watched': 0, 'is_rewatching': False,
             'num_times_rewatched': 0, 'rewatch_value': 0, 'priority': 0, 'tags': [], 'comments': 'Keep'}
    media = {'id': 1, 'title': 'Anime', 'media_type': 'tv', 'num_episodes': 12}
    case = SimpleNamespace(entry=entry, writes=[], timeout=False, store=StatusStore(tmp_path / 'mal-status.sqlite3'))
    def save(ident, before, patch):
        case.writes.append(deepcopy(patch))
        entry.update(patch)
        if case.timeout:
            raise TimeoutError('applied but response lost')
    client = SimpleNamespace(entries=lambda: [(entry, media)], media=lambda ident: {**media, 'my_list_status': deepcopy(entry)}, save=save)
    monkeypatch.setattr(StatusAdapter, 'client', lambda self: client)
    monkeypatch.setattr(service, 'StatusStore', lambda: case.store)
    monkeypatch.setattr(runtime, 'StatusStore', lambda: case.store)
    monkeypatch.setattr(snapshots, 'CONFIG', tmp_path)
    class Ops:
        def is_configured(self, cfg):
            return True

        def features(self):
            return {'history': True, 'ratings': True}

        def build_index(self, cfg, *, feature):
            return {}

        def add(self, cfg, items, *, feature, dry_run=False):
            _history._add(SimpleNamespace(raw_cfg={}, client=client), {'type': 'episode'}, '1', 2, entry, media)
            return {'ok': True, 'count': len(items)}
    monkeypatch.setattr(snapshots, 'load_sync_ops', lambda _: Ops())
    case.cfg = {'myanimelist': {}}
    return case


@pytest.mark.parametrize('feature', ['history', 'all'])
def test_real_mal_missing_comments_is_not_a_successful_status_capture(mal_case, feature):
    mal_case.entry.pop('comments')
    if feature == 'history':
        with pytest.raises(ValueError, match='incomplete observations'):
            snapshots.create_snapshot('MYANIMELIST', feature, cfg=mal_case.cfg)
    else:
        result = snapshots.create_snapshot('MYANIMELIST', feature, cfg=mal_case.cfg)
        assert not result['ok'] and 'title_statuses' not in result['stats']


def test_real_mal_history_writer_cannot_overwrite_pending_status(mal_case, tmp_path):
    import json
    case = mal_case
    payload = service.capture_statuses(case.cfg, 'MYANIMELIST', 'default')
    case.entry['status'] = 'watching'
    case.timeout = True
    result = service.restore_statuses(case.cfg, 'MYANIMELIST', 'default', payload, store=case.store)
    assert not result['ok'] and case.entry['status'] == 'dropped'
    pending = case.store.pending_targets()
    assert pending
    case.store = StatusStore(case.store.path)
    case.timeout = False
    path = tmp_path / 'snapshots' / 'mal.json'
    path.parent.mkdir()
    path.write_text(json.dumps({'provider': 'MYANIMELIST', 'feature': 'history',
                               'items': {'episode:1': {'type': 'episode', 'show_ids': {'mal': '1'}, 'season': 1, 'episode': 2}},
                               'title_statuses': payload}))
    result = snapshots.restore_snapshot('mal.json', cfg=case.cfg)
    assert not result['ok'] and any('still pending' in error for error in result['errors'])
    assert case.entry['status'] == 'dropped' and case.entry['num_episodes_watched'] == 0
    assert case.writes == [{'status': 'dropped'}]
    assert case.store.pending_targets() == pending


def test_full_restore_does_not_run_status_stage_after_feature_result_failure(case):
    case.feature_rows['history'] = {'movie:1': {'type': 'movie', 'ids': {'tmdb': '1'}, 'title': 'Movie'}}
    capture = snapshots.create_snapshot('SIMKL', 'all', cfg=case.cfg)
    case.feature_rows.clear()
    case.rows['1'] = record(Status.WATCHING)
    case.operation_result = {'ok': False, 'count': 0, 'unresolved': [{'reason': 'failed'}]}
    result = snapshots.restore_snapshot(capture['path'], cfg=case.cfg)
    assert not result['ok'] and result['added'] == 0
    assert not case.writes and 'title_statuses' not in result
    assert any('unresolved' in error for error in result['errors'])


def test_pending_is_rechecked_between_feature_batches(case, tmp_path, monkeypatch):
    import json
    path, _ = feature_capture(case, tmp_path, statuses=False)
    file = tmp_path / 'snapshots' / path
    payload = json.loads(file.read_text())
    payload['items']['movie:2'] = {'type': 'movie', 'ids': {'tmdb': '2'}, 'title': 'Second'}
    file.write_text(json.dumps(payload))
    chunks = snapshots._chunk
    def uncertain_between_batches(items, size):
        for index, batch in enumerate(chunks(items, size)):
            if index:
                case.rows['1'] = record(Status.WATCHING)
                case.fault = 'timeout'
                assert not service.restore_statuses(case.cfg, 'SIMKL', 'default', case.payload, store=case.store)['ok']
            yield batch
    monkeypatch.setattr(snapshots, '_chunk', uncertain_between_batches)
    result = snapshots.restore_snapshot(path, cfg=case.cfg, chunk_size=1)
    assert not result['ok'] and result['added'] == 1
    assert len(case.mutations) == 1
    assert any('still pending' in error for error in result['errors'])


@pytest.mark.parametrize('first', ['history', 'status'])
@pytest.mark.parametrize('operation', ['add', 'remove'])
def test_concurrent_restores_exclude_status_writes_from_feature_mutation(case, tmp_path, monkeypatch, first, operation):
    import json
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    history_path, row = feature_capture(case, tmp_path, statuses=False)
    status_path = tmp_path / 'snapshots' / 'status-only.json'
    status_path.write_text(json.dumps({'provider': 'SIMKL', 'feature': 'history', 'items': {}, 'title_statuses': case.payload}))
    case.rows['1'] = record(Status.WATCHING)
    case.fault = 'applied_timeout'
    entered, release, second_started = Event(), Event(), Event()
    ops = snapshots.load_sync_ops('SIMKL')
    mutate = getattr(ops, operation)
    def history_mutation(cfg, items, **kwargs):
        if first == 'history':
            entered.set()
            assert release.wait(5)
        result = mutate(cfg, items, **kwargs)
        case.rows['1'] = record(Status.WATCHING)
        return result
    monkeypatch.setattr(ops, operation, history_mutation)
    monkeypatch.setattr(snapshots, 'load_sync_ops', lambda _: ops)
    if operation == 'remove':
        case.feature_rows['history'] = {'movie:1': row}
        (tmp_path / 'snapshots' / history_path).write_text(json.dumps({'provider': 'SIMKL', 'feature': 'history', 'items': {}}))
        status_path.write_text(json.dumps({'provider': 'SIMKL', 'feature': 'history', 'items': {'movie:1': row}, 'title_statuses': case.payload}))
    cls = service.adapter_class('simkl')
    write = cls.write
    def status_mutation(self, record, desired):
        if first == 'status':
            entered.set()
            assert release.wait(5)
        return write(self, record, desired)
    monkeypatch.setattr(cls, 'write', status_mutation)
    def run(which):
        if which != first:
            second_started.set()
        return snapshots.restore_snapshot(history_path if which == 'history' else 'status-only.json', cfg=case.cfg,
                                          mode='clear_restore' if which == 'history' and operation == 'remove' else 'merge')
    with ThreadPoolExecutor(max_workers=2) as executor:
        a = executor.submit(run, first)
        try:
            assert entered.wait(5)
            b = executor.submit(run, 'status' if first == 'history' else 'history')
            assert second_started.wait(5)
            from concurrent.futures import TimeoutError as FutureTimeout
            with pytest.raises(FutureTimeout):
                b.result(timeout=0.4)
            assert not case.mutations and not case.writes
        finally:
            release.set()
        results = {first: a.result(timeout=5), ('status' if first == 'history' else 'history'): b.result(timeout=5)}
    assert not results['status']['ok']
    assert results['history']['ok'] == (first == 'history')
    assert len(case.mutations) == (1 if first == 'history' else 0)
    assert case.rows['1'].observation.status == Status.DROPPED
    assert case.store.pending_targets()
