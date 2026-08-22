# /cli/_errors.py
# CrossWatch - CLI error types and exit codes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_UNREACHABLE = 3
EXIT_UNAUTHORIZED = 4
EXIT_NOT_FOUND = 5
EXIT_BUSY = 6


class CLIError(Exception):
    def __init__(self, message: str, *, hint: str = "", exit_code: int = EXIT_ERROR) -> None:
        super().__init__(message)
        self.message = message
        self.hint = hint
        self.exit_code = exit_code


class TransportUnavailable(CLIError):
    def __init__(self, message: str, *, hint: str = "") -> None:
        super().__init__(message, hint=hint, exit_code=EXIT_UNREACHABLE)


class LocalUnsupported(CLIError):
    def __init__(self, what: str) -> None:
        super().__init__(
            f"{what} needs the CrossWatch service",
            hint="Start CrossWatch, or drop --local so the CLI can reach the running instance.",
            exit_code=EXIT_UNREACHABLE,
        )


class ApiError(CLIError):
    def __init__(self, status: int, payload: Any, *, method: str = "", path: str = "") -> None:
        self.status = int(status)
        self.payload = payload
        detail = ""
        if isinstance(payload, dict):
            for field in ("message", "detail", "error"):
                value = payload.get(field)
                if isinstance(value, str) and value.strip():
                    detail = value.strip()
                    break
        elif isinstance(payload, str):
            detail = payload.strip()[:400]
        where = f"{method} {path}".strip()
        message = detail or f"Request failed with HTTP {self.status}"
        if where:
            message = f"{message} ({where})"
        code = EXIT_ERROR
        hint = ""
        setup_required = detail.lower() == "authentication setup required"
        if setup_required:
            code = EXIT_UNAUTHORIZED
            hint = "Run 'cw --local auth setup --username admin' from the CrossWatch host or container to set the admin password and create a CLI token."
        elif self.status == 401:
            code = EXIT_UNAUTHORIZED
            hint = "Run 'cw --local auth token create' on the CrossWatch host/container; it saves the token by default. For a token from another machine, use CW_TOKEN or 'cw auth token use <token>'."
        elif self.status == 403:
            code = EXIT_UNAUTHORIZED
            hint = "This token does not have permission for that action."
        elif self.status == 404:
            code = EXIT_NOT_FOUND
        elif self.status == 409:
            code = EXIT_BUSY
        super().__init__(message, hint=hint, exit_code=code)
