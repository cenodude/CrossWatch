# cw_platform/log_context.py
# CrossWatch - Task-local log attribution
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from contextvars import ContextVar

log_run_id: ContextVar[str] = ContextVar('log_run_id', default='')
log_pair_id: ContextVar[str] = ContextVar('log_pair_id', default='')
