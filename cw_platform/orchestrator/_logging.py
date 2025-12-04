from __future__ import annotations
from typing import Any, Callable

class Emitter:
    def __init__(self, cb: Callable[[str], None] | None):
        self.cb = cb

    def emit(self, event: str, **data):
        if not self.cb:
            return
        try:
            payload = {"event": event}
            payload.update(data)
            self.cb(__import__("json").dumps(payload, separators=(",", ":")))
        except Exception:
            pass

    def info(self, line: str):
        if not self.cb:
            return
        try:
            self.cb(line)
        except Exception:
            pass

    def dbg(self, *args, **fields):
        if not args:
            return
        if isinstance(args[0], bool):
            enabled = args[0]
            msg = str(args[1]) if len(args) > 1 else ""
            extras = args[2:]
        else:
            enabled = True
            msg = str(args[0])
            extras = args[1:]

        if not enabled:
            return

        if extras:
            msg = " ".join([msg] + [str(x) for x in extras])

        if fields:
            self.emit("debug", msg=msg, **fields)
        else:
            self.info(f"[DEBUG] {msg}")
