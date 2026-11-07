# tests/test_status_instance_lock.py
# CrossWatch - Cross-process provider write exclusion and cancellation.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from threading import Barrier, Event
import subprocess
import sys
import time

import pytest

from cw_platform.run_control import SyncCancelled, clear_cancel, request_cancel
from cw_platform.status_mapping import Endpoint
from cw_platform.status_mapping_store import StatusStore


def test_instance_lock_is_shared_between_store_objects_and_instances_are_independent(tmp_path):
    store = StatusStore(tmp_path / 'state.sqlite3')
    endpoint = Endpoint('simkl', 'default')
    started = Event()
    def wait():
        started.set()
        with StatusStore(store.path).exclusive_instances([endpoint]):
            return True
    with ThreadPoolExecutor(max_workers=1) as executor:
        with store.exclusive_instances([endpoint]):
            future = executor.submit(wait)
            assert started.wait(5)
            with pytest.raises(FutureTimeout):
                future.result(timeout=0.4)
            with StatusStore(store.path).exclusive_instances([Endpoint('simkl', 'P01')]):
                assert not future.done()
        assert future.result(timeout=5)


def test_opposite_pair_directions_take_locks_in_consistent_order(tmp_path):
    store = StatusStore(tmp_path / 'state.sqlite3')
    a, b = Endpoint('simkl', 'default'), Endpoint('trakt', 'default')
    ready = Barrier(2)
    active = []
    def run(endpoints):
        ready.wait(timeout=5)
        with store.exclusive_instances(endpoints):
            assert not active
            active.append(True)
            time.sleep(0.05)
            active.clear()
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run, endpoints) for endpoints in ([a, b, a], [b, a])]
        for future in futures:
            future.result(timeout=5)


def test_cancelled_wait_releases_previously_acquired_instances(tmp_path):
    store = StatusStore(tmp_path / 'state.sqlite3')
    a, b = Endpoint('anilist', 'default'), Endpoint('trakt', 'default')
    started = Event()
    def wait():
        started.set()
        with store.exclusive_instances([a, b]):
            pytest.fail('Cancelled waiter entered the operation')
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with store.exclusive_instances([b]):
                future = executor.submit(wait)
                assert started.wait(5)
                with pytest.raises(FutureTimeout):
                    future.result(timeout=0.4)
                request_cancel()
                with pytest.raises(SyncCancelled):
                    future.result(timeout=5)
    finally:
        clear_cancel()
    with store.exclusive_instances([a, b]):
        pass


def test_instance_lock_releases_on_failure(tmp_path):
    store = StatusStore(tmp_path / 'state.sqlite3')
    endpoint = Endpoint('simkl', 'default')
    with pytest.raises(RuntimeError):
        with store.exclusive_instances([endpoint]):
            raise RuntimeError('provider failed')
    with StatusStore(store.path).exclusive_instances([endpoint]):
        pass


PROCESS_PROBE = '''
import sys
from pathlib import Path
from cw_platform.status_mapping import Endpoint
from cw_platform.status_mapping_store import StatusStore
Path(sys.argv[2]).write_text('ready')
with StatusStore(Path(sys.argv[1])).exclusive_instances([Endpoint('simkl', 'default')]):
    print('acquired', flush=True)
    if sys.argv[3] == 'hold':
        Path(sys.argv[2]).write_text('locked')
        sys.stdin.readline()
'''


def test_separate_process_waits_and_process_exit_releases_lock(tmp_path):
    store = StatusStore(tmp_path / 'state.sqlite3')
    marker = tmp_path / 'ready.txt'
    args = [sys.executable, '-c', PROCESS_PROBE, str(store.path), str(marker), 'exit']
    with store.exclusive_instances([Endpoint('simkl', 'default')]):
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            with pytest.raises(subprocess.TimeoutExpired):
                process.communicate(timeout=0.5)
        except BaseException:
            process.kill()
            process.communicate(timeout=5)
            raise
    try:
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 0 and stdout.strip() == 'acquired', stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
    args[-1] = 'hold'
    process = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 5
        while marker.read_text() != 'locked' and time.monotonic() < deadline:
            assert process.poll() is None
            time.sleep(0.02)
        assert marker.read_text() == 'locked'
    finally:
        process.kill()
        process.communicate(timeout=5)
    args[-1] = 'exit'
    probe = subprocess.run(args, capture_output=True, text=True, timeout=5)
    assert probe.returncode == 0 and probe.stdout.strip() == 'acquired', probe.stderr
