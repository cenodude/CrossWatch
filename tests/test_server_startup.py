# /tests/test_server_startup.py
# CrossWatch - Server bind address and port regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import ast
import os
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest


@pytest.fixture()
def run_entrypoint(monkeypatch):
    path = Path(__file__).resolve().parents[1] / "crosswatch.py"
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    entrypoint = ast.Module(body=[tree.body[-1]], type_ignores=[])
    code = compile(entrypoint, str(path), "exec")
    monkeypatch.delenv("WEB_HOST", raising=False)
    monkeypatch.delenv("WEB_PORT", raising=False)
    main = Mock()

    def run(argv):
        monkeypatch.setattr(sys, "argv", ["crosswatch", *argv])
        exec(code, {"__name__": "__main__", "os": os, "main": main})
        return main

    return run


@pytest.mark.parametrize(
    ("env", "argv", "host", "port"),
    [
        ({}, [], "0.0.0.0", 8787),
        ({}, ["--host", "127.0.0.1", "--port", "9090"], "127.0.0.1", 9090),
        ({"WEB_HOST": "127.0.0.2", "WEB_PORT": "9091"}, [], "127.0.0.2", 9091),
        ({"WEB_HOST": "", "WEB_PORT": ""}, [], "0.0.0.0", 8787),
        (
            {"WEB_HOST": "127.0.0.2", "WEB_PORT": "9091"},
            ["--host", "0.0.0.0", "--port", "9090"],
            "0.0.0.0",
            9090,
        ),
        ({"WEB_PORT": "9091"}, ["--host", "127.0.0.1"], "127.0.0.1", 9091),
        ({"WEB_PORT": "invalid"}, ["--port", "9090"], "0.0.0.0", 9090),
    ],
)
def test_server_bind_options(run_entrypoint, monkeypatch, env, argv, host, port):
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    main = run_entrypoint(argv)

    main.assert_called_once_with(host=host, port=port)


@pytest.mark.parametrize("argv", [["--port", "invalid"], []])
def test_server_rejects_invalid_port(run_entrypoint, monkeypatch, capsys, argv):
    monkeypatch.setenv("WEB_PORT", "invalid")

    with pytest.raises(SystemExit) as exc:
        run_entrypoint(argv)

    assert exc.value.code == 2
    assert "--port" in capsys.readouterr().err
