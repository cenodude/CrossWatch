# providers/scrobble/_log_dedupe.py
# CrossWatch - Bounded state for suppressing unchanged watcher diagnostics
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Hashable


class LogDeduplicator:
    def __init__(self, *, max_entries: int = 1024, idle_seconds: float = 1800.0) -> None:
        self._entries: OrderedDict[Hashable, tuple[Hashable, float]] = OrderedDict()
        self._max_entries = max_entries
        self._idle_seconds = idle_seconds
        self._lock = threading.Lock()

    def should_log(self, key: Hashable, signature: Hashable) -> bool:
        now = time.monotonic()
        with self._lock:
            previous = self._entries.get(key)
            self._entries[key] = (signature, now)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)
            return not (
                previous and previous[0] == signature
                and 0 <= now - previous[1] < self._idle_seconds
            )

    def discard(self, key: Hashable) -> None:
        with self._lock:
            self._entries.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
