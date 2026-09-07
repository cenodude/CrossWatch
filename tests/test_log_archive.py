# tests/test_log_archive.py
# CrossWatch - Log archive persistence, retention and access tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

import time
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.log_archive import LogArchive, describe_line
from cw_platform.log_context import log_run_id, log_pair_id
from cw_platform.orchestrator._pairs import _pair_env


@pytest.mark.parametrize("text,tag,expected", [
    ("[MDBLIST:history] INFO index_done", "SYNC", "MDBLIST"),
    ("[SYNC] [PLEX-WATCH] INFO playing", "SYNC", "PLEX-WATCH"),
    ("[SIMKL:history:episodes] DEBUG read", "SYNC", "SIMKL"),
    ("[A:" * 10000, "SYNC", "SYNC"),
    ("[A:" * 10000 + "[TRAKT:history] INFO read", "SYNC", "TRAKT"),
    ("[A:" * 10000 + "]", "SYNC", "SYNC"),
    ('[SYNC] {"provider":"MDBLIST","event":"debug"}', "SYNC", "MDBLIST"),
])
def test_provider_tags_handle_valid_and_unclosed_brackets(text, tag, expected):
    assert describe_line(text, tag)[2] == expected


@pytest.fixture
def store(tmp_path):
    result = LogArchive(tmp_path / 'logs.db')
    yield result
    result.close()


def test_saved_run_survives_restart_and_interrupted_run_is_marked(tmp_path):
    path = tmp_path / 'logs.db'
    log = LogArchive(path)
    sid = log.start('run-one', [{'source':'PLEX','target':'TRAKT'}])
    log.append('sync', 'SYNC', '[TRAKT] ERROR token=secret could not write')
    log.close()
    log = LogArchive(path)
    try:
        assert log.session(sid)['status'] == 'interrupted'
        assert log.session(sid)['errors'] == 1
        assert 'secret' not in log.page(sid)['items'][0]['text']
        assert log.page(sid)['items'][0]['provider'] == 'TRAKT'
    finally:
        log.close()


def test_full_search_paging_and_context_ignore_filters(store):
    sid = store.start('run', [])
    for index in range(16):
        store.append('sync', 'PLEX', f'{"ERROR" if index == 8 else "INFO"} item {index}')
    result = store.page(sid, q='ITEM 8', level='ERROR', provider='PLEX')
    assert result['total'] == 1
    context = store.context(sid, result['items'][0]['id'])
    assert len(context) == 11
    assert context[0]['text'] == 'INFO item 3'
    assert context[-1]['text'] == 'INFO item 13'
    assert len(store.page(sid, offset=10, limit=5)['items']) == 5


def test_retention_skips_pinned_and_active_logs(store):
    old = store.start('old', [])
    store.finish(old, 'completed')
    keep = store.start('keep', [])
    store.finish(keep, 'completed')
    store.pin(keep, True)
    active = store.start('active', [])
    store.cleanup(before=time.time()+1)
    assert store.session(old) is None
    assert store.session(keep)['pinned'] == 1
    assert store.session(active)['ended'] is None
    with pytest.raises(ValueError):
        store.delete(active)


def test_storage_cap_keeps_pin_and_marks_truncated(tmp_path):
    log = LogArchive(tmp_path / 'small.db', max_bytes=1800)
    try:
        keep = log.start('keep', [])
        log.append('sync', 'SYNC', 'INFO ' + 'x'*400)
        log.finish(keep, 'completed')
        log.pin(keep, True)
        sid = log.start('next', [])
        for _ in range(10):
            log.append('sync', 'SYNC', 'ERROR ' + 'x'*200)
        assert log.used <= 1800
        assert log.session(keep)['pinned'] == 1
        assert log.session(sid)['truncated'] == 1
    finally:
        log.close()


def test_old_unpinned_log_is_evicted_to_make_room(tmp_path):
    log = LogArchive(tmp_path/'small.db', max_bytes=1600)
    try:
        old = log.start('old', [])
        log.append('sync', 'SYNC', 'x'*400)
        log.finish(old, 'completed')
        new = log.start('new', [])
        log.append('sync', 'SYNC', 'x'*400)
        assert log.session(old) is None
        assert log.page(new)['total'] == 1
    finally:
        log.close()


def test_daily_channels_and_completed_run_remain_separate(store):
    sid=store.start('sync', [])
    store.append('sync','SYNC','INFO run')
    store.append('watcher','PLEX-WATCH','INFO playing')
    store.append('debug','DEBUG','DEBUG diagnostic')
    store.finish(sid,'completed')
    store.append('sync','SYNC','INFO outside run')
    assert store.page(sid)['total'] == 1
    assert len(store.sessions('watcher')) == 1
    assert len(store.sessions('debug')) == 1
    assert len(store.sessions('sync')) == 2


def test_structured_error_levels():
    assert describe_line('{"event":"apply:update:done","errors":2}', 'SYNC')[1] == 'ERROR'
    assert describe_line('{"event":"debug","msg":"snapshot"}', 'SYNC')[1] == 'DEBUG'
    assert describe_line('{"event":"run:done","unresolved":1}', 'SYNC')[1] == 'WARN'
    assert describe_line('{"event":"run:done","errors":"0","unresolved":"0"}', 'SYNC')[1] == 'INFO'
    assert describe_line('[MDBLIST] INFO [MDBLIST:history] DEBUG index_cache_hit', 'SYNC')[1] == 'DEBUG'
    assert describe_line('INFO Done. Total errors: 0', 'SYNC')[1] == 'INFO'
    assert describe_line('{"provider":"SIMKL","level":"WARN","msg":"rate_limited"}', 'DEBUG')[1:] == ('WARN', 'SIMKL')


def test_api_scopes_every_read_export_pin_and_delete(monkeypatch, store):
    from api import logsAPI
    monkeypatch.setattr(logsAPI, 'archive', lambda:store)
    monkeypatch.setattr(logsAPI, 'load_config', lambda:{})
    monkeypatch.setattr(logsAPI, 'user_can_access_instance', lambda cfg,user,provider,instance:provider=='PLEX')
    mine=store.start('mine', [{'id':'mine','source':'PLEX','target':'PLEX'}]);store.append('sync','SYNC','INFO own log');store.finish(mine,'completed')
    theirs=store.start('theirs',[{'id':'theirs','source':'TRAKT','target':'TRAKT'}]);store.append('sync','SYNC','ERROR private log');store.finish(theirs,'failed')
    mixed=store.start('mixed',[{'source':'PLEX','target':'PLEX'},{'source':'TRAKT','target':'TRAKT'}]);store.finish(mixed,'completed')
    store.append('debug','DEBUG','INFO private diagnostics')
    app=FastAPI()
    @app.middleware('http')
    async def user(request,call_next):
        request.state.cw_user={'is_admin':False,'profile_id':'a'*32,'permissions':{'write':True}}
        return await call_next(request)
    app.include_router(logsAPI.router)
    with TestClient(app) as client:
        data=client.get('/api/logs/archive').json()
        assert [row['id'] for row in data['items']] == [mine]
        assert data['channels']==['sync']
        assert [pair['id'] for pair in data['pairs']] == ['mine']
        assert client.get('/api/logs/archive?pair_id=theirs').json()['latest_run_id'] == ''
        assert client.get('/api/logs/archive?channel=debug').json()['items']==[]
        for sid in (theirs,mixed):
            assert client.get(f'/api/logs/archive/{sid}/lines').status_code==404
            assert client.get(f'/api/logs/archive/{sid}/lines?pair_id=mine').status_code==404
            assert client.get(f'/api/logs/archive/{sid}/context/1').status_code==404
            assert client.get(f'/api/logs/archive/{sid}/download').status_code==404
            assert client.patch(f'/api/logs/archive/{sid}',json={'pinned':True}).status_code==404
            assert client.delete(f'/api/logs/archive/{sid}').status_code==404
        assert client.get(f'/api/logs/archive/{mine}/download').text.endswith('[SYNC] INFO INFO own log\n')
        assert client.patch(f'/api/logs/archive/{mine}',json={'pinned':True}).status_code==200
        assert client.delete('/api/logs/archive').json()['deleted']==0
        assert store.session(theirs) is not None
        assert client.delete(f'/api/logs/archive/{mine}').status_code==200


def test_export_batches_and_applies_filters(store):
    sid = store.start('export', [])
    for i in range(1010):
        store.append('sync', 'PLEX', f'INFO item {i}')
    assert len(list(store.export(sid))) == 1010
    result = list(store.export(sid, q='item 1009', level='INFO', provider='PLEX'))
    assert len(result) == 1
    assert '+00:00 [PLEX] INFO INFO item 1009' in result[0]


def record_pair_run(store, run_id, pairs):
    sid = store.start(run_id, pairs)
    token = log_run_id.set(run_id)
    try:
        store.append('sync', 'SYNC', 'INFO shared preparation')
        for i, pair in enumerate(pairs):
            store.append('sync', 'SYNC', json.dumps({'event':'run:pair', 'pair_id':pair['id']}))
            with _pair_env(pair, i=i, src=pair['source'], dst=pair['target'], mode='one-way', feature='history'):
                store.append('sync', 'SYNC', f"ERROR {pair['id']} failed")
                store.append('debug', 'DEBUG', f"DEBUG {pair['id']} detail for {run_id}")
                # A Watcher/HTTP worker running at the same time has no sync context.
                with ThreadPoolExecutor(max_workers=1) as executor:
                    executor.submit(store.append, 'debug', 'DEBUG', 'INFO unrelated background').result()
            assert log_pair_id.get() == ''
        store.finish(sid, 'failed')
    finally:
        log_run_id.reset(token)
    return sid


def test_pair_filters_distinguish_identical_routes_and_keep_context_and_exports_scoped(store):
    pairs = [dict(id=pid, source='PLEX', target='TRAKT') for pid in ('a','b')]
    sid = record_pair_run(store, 'run-one', pairs)
    rows = store.page(sid, pair_id='a')['items']
    assert len(rows) == 2
    assert {r['pair_id'] for r in rows} == {'a'}
    assert store.counts(sid, 'a')['errors'] == 1
    assert {r['pair_id'] for r in store.context(sid, rows[-1]['id'], 'a')} == {'a'}
    exported = ''.join(store.export(sid, pair_id='a'))
    assert 'a failed' in exported and 'b failed' not in exported and 'shared preparation' not in exported
    debug = store.sessions('debug')[0]['id']
    assert store.page(debug, pair_id='a')['total'] == 1
    assert store.page(debug, pair_id='b')['total'] == 1
    assert store.page(debug)['total'] == 4
    newer = record_pair_run(store, 'run-two', pairs[:1])
    assert store.sessions('sync', pair_id='a')[0]['id'] == newer
    assert store.sessions('sync', pair_id='b')[0]['id'] == sid
    assert store.page(debug, pair_id='a', run_id='run-two')['total'] == 1
    assert 'run-one' not in ''.join(store.export(debug, pair_id='a', run_id='run-two'))


def test_pair_api_latest_run_counts_context_and_debug_run(monkeypatch, store):
    from api import logsAPI
    pairs = [dict(id=pid, source='PLEX', target='TRAKT') for pid in ('a','b')]
    old = record_pair_run(store, 'run-one', pairs)
    new = record_pair_run(store, 'run-two', pairs[:1])
    monkeypatch.setattr(logsAPI, 'archive', lambda:store)
    monkeypatch.setattr(logsAPI, 'load_config', lambda:{'pairs':pairs})
    app = FastAPI(); app.include_router(logsAPI.router)
    with TestClient(app) as client:
        result = client.get('/api/logs/archive?pair_id=b').json()
        assert result['latest_run_id'] == 'run-one'
        assert [item['id'] for item in result['items']] == [old]
        assert {p['id'] for p in result['pairs']} == {'a','b'}
        assert result['items'][0]['errors'] == 1
        assert client.get('/api/logs/archive?pair_id=a').json()['latest_run_id'] == 'run-two'
        page = client.get(f'/api/logs/archive/{old}/lines?pair_id=b').json()
        assert {row['pair_id'] for row in page['items']} == {'b'}
        context = client.get(f"/api/logs/archive/{old}/context/{page['items'][0]['id']}?pair_id=b").json()
        assert {row['pair_id'] for row in context['items']} == {'b'}
        assert 'a failed' not in client.get(f'/api/logs/archive/{old}/download?pair_id=b').text
        debug = client.get('/api/logs/archive?channel=debug&pair_id=a&run_id=run-two').json()['items'][0]
        lines = client.get(f"/api/logs/archive/{debug['id']}/lines?pair_id=a&run_id=run-two").json()
        assert len(lines['items']) == 1
        assert lines['items'][0]['run_id'] == 'run-two'
        assert client.get('/api/logs/archive?pair_id=missing').json()['items'] == []
