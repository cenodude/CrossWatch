# /_backup_mapping.py
# CrossWatch - Selective mapping recovery without replacing other application state
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from pathlib import Path

_TABLES = ("manual_policy_features", "manual_policy_blocks", "manual_policy_add_items", "manual_policy_pairs")
_META_KEYS = ("manual_policy_version", "manual_policy_media_identity")


def has_mappings(path: Path) -> bool:
    with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        return set(_TABLES[:3]).issubset(tables)


def merge_mappings(source: Path, live: Path, output: Path) -> None:
    from cw_platform.local_db.schema import SCHEMA_VERSION, apply_schema

    with closing(sqlite3.connect(source)) as saved, closing(sqlite3.connect(output)) as restored:
        os.chmod(output, 0o600)
        saved.execute("PRAGMA trusted_schema=OFF")
        restored.execute("PRAGMA trusted_schema=OFF")
        if live.exists():
            with closing(sqlite3.connect(f"{live.as_uri()}?mode=ro", uri=True)) as current:
                current.backup(restored)
        for conn in (saved, restored):
            if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'").fetchone():
                version = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0]
                if version is not None and version > SCHEMA_VERSION:
                    raise ValueError("These mappings need a newer CrossWatch version.")
            apply_schema(conn)
        with restored:
            for table in reversed(_TABLES):
                restored.execute(f"DELETE FROM {table}")
            for table in _TABLES:
                columns = [row[1] for row in restored.execute(f"PRAGMA table_info({table})")]
                names = ",".join('"' + name.replace('"', '""') + '"' for name in columns)
                placeholders = ",".join("?" for _ in columns)
                rows = saved.execute(f"SELECT {names} FROM {table}")
                restored.executemany(f"INSERT INTO {table} ({names}) VALUES ({placeholders})", rows)
            for key in _META_KEYS:
                restored.execute("DELETE FROM local_meta WHERE key=?", (key,))
                row = saved.execute("SELECT key,value_text,value_int,value_real,value_type,updated_at FROM local_meta WHERE key=?", (key,)).fetchone()
                if row:
                    restored.execute("INSERT INTO local_meta(key,value_text,value_int,value_real,value_type,updated_at) VALUES(?,?,?,?,?,?)", row)
