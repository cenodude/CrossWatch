# cw_platform/local_db/operation_lock.py
# CrossWatch - Exclusive sync and database maintenance operations
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
import errno
import logging
from contextlib import contextmanager
from pathlib import Path

from .db import crosswatch_db_path


@contextmanager
def exclusive_operation(base_path: str | Path | None = None, *, wait: bool = False, cancelled=None, kind: str = "operation"):
    path = crosswatch_db_path(base_path).with_suffix(f".{kind}.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("a+b")
    acquired = False
    waiting = False
    try:
        if os.name == "nt":
            import msvcrt

            if path.stat().st_size == 0:
                stream.write(b"0")
                stream.flush()
            stream.seek(0)
            lock = lambda: msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            lock = lambda: fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            if cancelled and cancelled():
                from cw_platform.run_control import SyncCancelled

                raise SyncCancelled()
            try:
                lock()
                break
            except OSError as exc:
                if exc.errno not in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                    raise
                if not wait:
                    raise RuntimeError("database_operation_busy") from exc
                if not waiting:
                    logging.getLogger("crosswatch.local_db").info("Sync waiting for database maintenance to finish")
                    waiting = True
                time.sleep(.25)
        acquired = True
        yield
    finally:
        if acquired:
            if os.name == "nt":
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        stream.close()
