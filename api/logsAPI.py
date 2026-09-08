# api/logsAPI.py
# CrossWatch - Log archive API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

import json
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from cw_platform.access_policy import request_user, pair_refs, user_can_access_instance, requested_view_as_profile, profile_instances_map, profile_allows_instance
from cw_platform.config_base import load_config
from services.log_archive import archive, MAX_BYTES, RETENTION_DAYS

router = APIRouter(prefix='/api/logs/archive', tags=['logging'])


def debug_enabled(cfg: dict) -> bool:
    runtime = cfg.get('runtime') or {}
    return bool(runtime.get('debug') or runtime.get('debug_mods'))


def allowed(request: Request, session: dict) -> bool:
    if session['channel'] == 'debug' and not debug_enabled(load_config() or {}):
        return False
    user = request_user(request)
    profile = requested_view_as_profile(request)
    if (not user or user.get('is_admin')) and not profile:
        return True
    if session['channel'] != 'sync':
        return False
    pairs = json.loads(session['pairs'])
    if not pairs:
        return False
    cfg = load_config() or {}
    scope = profile_instances_map(cfg, profile) if profile else None
    refs = [ref for pair in pairs for ref in pair_refs(pair)]
    return bool(refs) and all(user_can_access_instance(cfg, user, provider, instance)
                              and (scope is None or profile_allows_instance(scope, provider, instance))
                              for provider, instance in refs)


def get_session(request: Request, sid: str) -> dict:
    item = archive().session(sid)
    if not item or not allowed(request, item):
        raise HTTPException(404, 'Log not found for this profile.')
    return item


@router.get('')
def sessions(request: Request, channel: Literal['sync', 'watcher', 'debug'] = 'sync', run_id: str = Query('', max_length=160), pair_id: str = Query('', max_length=160)):
    store = archive()
    items = [item for item in store.sessions(channel, run_id, pair_id) if allowed(request, item)]
    sync_logs = [item for item in store.sessions('sync') if allowed(request, item)]
    choices = {}
    # Include configured pairs with no logs yet, and retained logs for removed pairs.
    cfg = load_config() or {}
    configured = cfg.get('pairs') or []
    for pair in configured:
        if isinstance(pair, dict) and allowed(request, dict(channel='sync', pairs=json.dumps([pair]))):
            choices[str(pair.get('id') or '')] = pair
    for item in sync_logs:
        for pair in json.loads(item['pairs']):
            choices.setdefault(str(pair.get('id') or ''), pair)
    choices.pop('', None)
    pairs = []
    for index, (key, pair) in enumerate(choices.items(), 1):
        (src, src_inst), (dst, dst_inst) = pair_refs(pair)
        arrow = '↔' if 'two' in str(pair.get('mode') or '') else '→'
        label = f"{pair.get('name') or f'Pair {index}'} · {src} · {src_inst} {arrow} {dst} · {dst_inst}"
        pairs.append(dict(id=key, label=label))
    latest = next((item['run_id'] for item in store.sessions('sync', pair_id=pair_id) if item['run_id'] and allowed(request, item)), '')
    if pair_id or run_id:
        items = [dict(item, **store.counts(item['id'], pair_id, run_id)) for item in items]
    user = request_user(request)
    admin = (not user or bool(user.get('is_admin'))) and not requested_view_as_profile(request)
    channels = ['sync', 'watcher'] if admin else ['sync']
    if admin and debug_enabled(cfg):
        channels.append('debug')
    return dict(items=items, pairs=pairs, latest_run_id=latest, channels=channels, retention_days=RETENTION_DAYS,
                max_bytes=MAX_BYTES, used_bytes=archive().used if admin else sum(item['bytes'] for item in items))


@router.get('/{sid}/lines')
def lines(request: Request, sid: str, q: str = Query('', max_length=200), level: str = Query('', pattern='^(|ERROR|WARN|INFO|DEBUG)$'),
          provider: str = Query('', max_length=80), after: int = Query(0, ge=0), offset: int = Query(0, ge=0), limit: int = Query(500, ge=1, le=1000),
          pair_id: str = Query('', max_length=160), run_id: str = Query('', max_length=160)):
    item = get_session(request, sid)
    if pair_id or run_id:
        item = dict(item, **archive().counts(sid, pair_id, run_id))
    result = archive().page(sid, q=q, level=level, provider=provider, after=after, offset=offset, limit=limit, pair_id=pair_id, run_id=run_id)
    return dict(session=item, **result)


@router.get('/{sid}/context/{line_id}')
def context(request: Request, sid: str, line_id: int, pair_id: str = Query('', max_length=160), run_id: str = Query('', max_length=160)):
    get_session(request, sid)
    return dict(items=archive().context(sid, line_id, pair_id, run_id))


class Pin(BaseModel):
    pinned: bool


@router.patch('/{sid}')
def pin(request: Request, sid: str, body: Pin):
    get_session(request, sid)
    archive().pin(sid, body.pinned)
    return dict(ok=True)


@router.delete('/{sid}')
def delete(request: Request, sid: str):
    get_session(request, sid)
    try:
        archive().delete(sid)
    except ValueError as error:
        raise HTTPException(409, str(error)) from error
    return dict(ok=True)


@router.delete('')
def clear_older(request: Request, channel: Literal['sync', 'watcher', 'debug'] = 'sync'):
    count = 0
    for item in archive().sessions(channel):
        if item['ended'] is not None and not item['pinned'] and allowed(request, item):
            archive().delete(item['id'])
            count += 1
    return dict(ok=True, deleted=count)


@router.get('/{sid}/download', response_class=StreamingResponse)
def download(request: Request, sid: str, q: str = Query('', max_length=200), level: str = Query('', pattern='^(|ERROR|WARN|INFO|DEBUG)$'), provider: str = Query('', max_length=80),
             pair_id: str = Query('', max_length=160), run_id: str = Query('', max_length=160)):
    get_session(request, sid)
    return StreamingResponse(archive().export(sid, q=q, level=level, provider=provider, pair_id=pair_id, run_id=run_id), media_type='text/plain',
                             headers={'Content-Disposition': f'attachment; filename="crosswatch-{sid}.log"', 'Cache-Control': 'no-store'})
