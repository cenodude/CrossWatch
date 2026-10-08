# services/interactive_sync_export.py
# CrossWatch - Interactive Sync Review Downloads
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import csv
import io
import json
import tempfile
import time

from .interactive_sync_store import ReviewStore


@dataclass
class ExportSnapshot:
    metadata: dict
    store: ReviewStore


def capture_export(session, *, scope="all", feature="", result="", q=""):
    metadata = deepcopy(dict(schema_version=1, session_id=session.id, pair_id=session.pair_id, pair=session.pair,
                             planned_at=session.planned_at, exported_at=int(time.time()), revision=session.revision,
                             selection_version=session.selection_version, scope=scope,
                             filters=dict(feature=feature, result=result, q=q) if scope == "filtered" else {},
                             counts=session.store.counts, notices=session.plan.notices, choices=session.plan.choices))
    return ExportSnapshot(metadata=metadata, store=session.store.snapshot())


COLUMNS = (
    "source", "source_instance", "destination", "destination_instance", "feature", "operation",
    "media_type", "title", "series_title", "year", "season", "episode", "provider_ids", "show_ids",
    "result", "reason", "selected", "selectable", "row_id", "key", "destination_label",
    "before", "proposed_item", "conflict_left", "conflict_right", "conflict_winner",
)


def csv_cell(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list, bool)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    text = str(value)
    if text.lstrip().startswith(("=", "+", "-", "@")) or text.startswith(("\t", "\r", "\n")):
        return "'" + text
    return text


def csv_row(row):
    item = row.get("item") or {}
    values = (
        row.get("source"), row.get("source_instance"), row.get("provider", row.get("target")),
        row.get("instance"), row.get("feature"), row.get("operation"), item.get("type"),
        item.get("title") or item.get("name"), item.get("series_title"), item.get("year"),
        item.get("season"), item.get("episode"), item.get("ids"), item.get("show_ids"),
        row.get("result"), row.get("reason"), row.get("selected"), row.get("selectable"),
        row.get("id"), row.get("key"), row.get("destination_label"), row.get("before"), item,
        row.get("left"), row.get("right"), row.get("winner"),
    )
    return [csv_cell(value) for value in values]


def build_export(snapshot, *, format="csv"):
    output = tempfile.SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b")
    try:
        metadata = snapshot.metadata
        rows = snapshot.store.export_rows(scope=metadata["scope"], **metadata["filters"])
        if format == "json":
            output.write((json.dumps(metadata, ensure_ascii=False, default=str)[:-1] + ', "rows":[').encode("utf-8"))
            total = 0
            for row in rows:
                if total:
                    output.write(b",")
                output.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                total += 1
            output.write(('], "exported_rows":' + str(total) + '}').encode("utf-8"))
        else:
            output.write(b"\xef\xbb\xbf")
            buffer = io.StringIO(newline="")
            writer = csv.writer(buffer)
            writer.writerow(COLUMNS)
            output.write(buffer.getvalue().encode("utf-8"))
            for row in rows:
                buffer.seek(0)
                buffer.truncate()
                writer.writerow(csv_row(row))
                output.write(buffer.getvalue().encode("utf-8"))
        output.seek(0)
        return output
    except BaseException:
        output.close()
        raise


def export_chunks(output):
    try:
        while chunk := output.read(65536):
            yield chunk
    finally:
        output.close()
