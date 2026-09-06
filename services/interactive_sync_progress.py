# services/interactive_sync_progress.py
# CrossWatch - Interactive Sync Progress Tracking
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections import deque
import json
import threading
import time
from typing import Any, overload


class SyncProgress:
    def __init__(self):
        self.lock = threading.RLock()
        self.begin("preview")

    def begin(self, operation):
        with self.lock:
            self.operation = operation
            self.started = self.changed = self.activity = time.monotonic()
            self.finished = None
            self.stage = "connecting" if operation == "preview" else "verifying"
            self.label = "Checking provider connections" if operation == "preview" else "Rechecking providers before applying"
            self.provider = self.feature = ""
            self.done, self.total, self.unit = 0, None, "items"
            self.requests = 0
            self.reads = {}
            self.recent = deque(maxlen=6)
            self.recent.append(dict(at=time.time(), text=self.label))

    def set_stage(self, stage, label, *, provider="", feature="", done=0, total=None, unit="items"):
        with self.lock:
            identity = (stage, label, provider, feature)
            if identity != (self.stage, self.label, self.provider, self.feature):
                self.changed = time.monotonic()
                self.recent.append(dict(at=time.time(), text=label))
            self.stage, self.label, self.provider, self.feature = identity
            self.done, self.total, self.unit = done, total, unit
            self.activity = time.monotonic()

    def finish(self, status, message):
        with self.lock:
            self.set_stage(status, message)
            self.finished = time.monotonic()

    @staticmethod
    @overload
    def number(value: Any, default: int = 0) -> int: ...

    @staticmethod
    @overload
    def number(value: Any, default: None) -> int | None: ...

    @staticmethod
    def number(value: Any, default: int | None = 0) -> int | None:
        try:
            return max(0, int(value))
        except (ValueError, TypeError, OverflowError):
            return default

    def event(self, event):
        if isinstance(event, str):
            try:
                event = json.loads(event)
            except (ValueError, TypeError):
                return
        if not isinstance(event, dict):
            return
        name = str(event.get("event") or "")
        feature = str(event.get("feature") or "")[:32]
        provider = str(event.get("provider") or event.get("dst") or "")[:80]
        done = self.number(event.get("done"))
        total = self.number(event.get("total"), None)
        subject = " ".join(x for x in (provider, feature) if x) or "provider data"
        with self.lock:
            if self.finished is not None:
                return
            if name == "api:request":
                self.requests += 1
                self.activity = time.monotonic()
                return
            if name == "health":
                self.set_stage("connecting" if self.operation == "preview" else "verifying", f"Checked {provider} connection", provider=provider)
            elif name in ("snapshot:start", "snapshot:progress"):
                stage = "reading" if self.operation == "preview" else "verifying"
                self.set_stage(stage, f"{'Reading' if self.operation == 'preview' else 'Rechecking'} {subject}", provider=provider, feature=feature, done=done, total=total, unit="items read")
                self.reads[(provider, feature)] = max(self.reads.get((provider, feature), 0), done)
            elif name == "snapshot:done":
                self.reads[(provider, feature)] = self.number(event.get("count"))
                self.set_stage("planning" if self.operation == "preview" else "verifying", f"Calculating {feature or 'sync'} changes", feature=feature)
            elif name == "review:plan":
                stage = "planning" if self.operation == "preview" else "verifying"
                self.set_stage(stage, f"{'Building' if self.operation == 'preview' else 'Verifying'} {feature} changes for {provider}", provider=provider, feature=feature, done=done, total=total, unit="changes checked")
            elif name in ("apply:add:start", "apply:update:start", "apply:remove:start"):
                operation = name.split(":")[1]
                self.set_stage("applying", f"{'Removing from' if operation == 'remove' else 'Updating' if operation == 'update' else 'Adding to'} {subject}", provider=provider, feature=feature, total=self.number(event.get("count")), unit="changes processed")
            elif name in ("apply:add:progress", "apply:update:progress", "apply:remove:progress", "apply:add:done", "apply:update:done", "apply:remove:done"):
                if name.endswith(":done"):
                    done = self.number(event.get("attempted"), self.total or self.number(event.get("count")))
                    total = self.total
                self.set_stage("applying", self.label if self.stage == "applying" else f"Processing {subject}", provider=provider, feature=feature, done=done, total=total, unit="changes processed")
            elif name in ("rate:slow", "rate:low", "rate_limit_retry", "http_retry"):
                self.recent.append(dict(at=time.time(), text=f"{provider or 'Provider'} is rate limiting or retrying requests"))
                self.activity = time.monotonic()
            elif name in ("feature:error", "feature:unsupported", "writes:skipped", "mass_delete:blocked", "snapshot:suspect", "playlist:mapping:error"):
                messages = {"feature:error": "Provider reported an error", "feature:unsupported": "Feature unavailable", "writes:skipped": "Provider writes skipped", "mass_delete:blocked": "Mass delete protection activated", "snapshot:suspect": "Provider data needs verification", "playlist:mapping:error": "Playlist mapping reported an error"}
                self.recent.append(dict(at=time.time(), text=f"{subject}: {messages[name]}"))
                self.activity = time.monotonic()
            elif name == "run:done":
                self.set_stage("finalizing", "Saving sync results")

    def public(self):
        with self.lock:
            now = self.finished or time.monotonic()
            percent = min(100, round(100 * self.done / self.total, 1)) if self.total else None
            return dict(operation=self.operation, stage=self.stage, label=self.label, provider=self.provider,
                        feature=self.feature, done=self.done, total=self.total, unit=self.unit, percent=percent,
                        elapsed_seconds=int(now - self.started), stage_seconds=int(now - self.changed),
                        quiet_seconds=int(now - self.activity), running=self.finished is None,
                        items_read=sum(self.reads.values()), requests=self.requests, recent=list(self.recent))
