# services/interactive_sync_store.py
# CrossWatch - Paged Interactive Sync Review Storage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import MutableMapping
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import weakref


class ReviewRows(MutableMapping):
    def __init__(self, store, kind):
        self.store = weakref.proxy(store)
        self.kind = kind

    def __getitem__(self, key):
        with self.store.lock:
            row = self.store.db.execute("SELECT payload FROM review WHERE id=? AND kind=?", (key, self.kind)).fetchone()
        if row is None:
            raise KeyError(key)
        return json.loads(row[0])

    def __setitem__(self, key, value):
        self.store.put(key, value, self.kind)

    def __delitem__(self, key):
        with self.store.lock:
            if not self.store.db.execute("DELETE FROM review WHERE id=? AND kind=?", (key, self.kind)).rowcount:
                raise KeyError(key)

    def __iter__(self):
        with self.store.lock:
            cursor = self.store.db.execute("SELECT id FROM review WHERE kind=? ORDER BY seq", (self.kind,))
            while batch := cursor.fetchmany(256):
                yield from (row[0] for row in batch)

    def __len__(self):
        with self.store.lock:
            return self.store.db.execute("SELECT COUNT(*) FROM review WHERE kind=?", (self.kind,)).fetchone()[0]


class ReviewStore:
    def __init__(self):
        descriptor, path = tempfile.mkstemp(prefix="cw-sync-review-", suffix=".sqlite")
        os.close(descriptor)
        self.path = Path(path)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.cleanup = weakref.finalize(self, self._cleanup, self.db, self.path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("PRAGMA cache_size=-2048")
        self.db.execute("PRAGMA temp_store=FILE")
        self.db.execute("CREATE TABLE review (seq INTEGER PRIMARY KEY, id TEXT UNIQUE NOT NULL, kind TEXT NOT NULL, feature TEXT NOT NULL, result TEXT NOT NULL, selectable INTEGER NOT NULL, selected INTEGER NOT NULL, search TEXT NOT NULL, payload TEXT NOT NULL)")
        self.rows = ReviewRows(self, "change")
        self.conflicts = ReviewRows(self, "conflict")
        self.counts = dict(changes=0, conflicts=0, attention=0, selected=0)
        self.features = []
        self.selectable_count = 0
        self.db.execute("CREATE TABLE report_issues (id TEXT PRIMARY KEY, provider TEXT NOT NULL, feature TEXT NOT NULL, result TEXT NOT NULL, operation TEXT NOT NULL, provisional INTEGER NOT NULL, search TEXT NOT NULL, payload TEXT NOT NULL)")

    def clear_report_issues(self, provider=None, feature=None, operation=None):
        with self.lock:
            if provider is None:
                self.db.execute("DELETE FROM report_issues")
            else:
                self.db.execute("DELETE FROM report_issues WHERE provider=? AND feature=? AND operation=? AND provisional=1", (provider, feature, operation))

    def put_report_issue(self, key, row, provisional=False):
        payload = json.dumps(row, ensure_ascii=False, default=str)
        with self.lock:
            self.db.execute("INSERT OR REPLACE INTO report_issues VALUES(?,?,?,?,?,?,?,?)",
                            (key, row["provider"], row["feature"], row["result"], row["operation"], int(provisional), payload.casefold(), payload))

    def report_page(self, *, offset=0, limit=75, feature="", result="", q=""):
        where, args = self.where(feature, result, q)
        with self.lock:
            total = self.db.execute(f"SELECT COUNT(*) FROM report_issues WHERE {where}", args).fetchone()[0]
            limit = max(1, min(200, int(limit)))
            offset = max(0, min(int(offset), ((total - 1) // limit) * limit)) if total else 0
            rows = self.db.execute(f"SELECT payload FROM report_issues WHERE {where} ORDER BY rowid LIMIT ? OFFSET ?", [*args, limit, offset]).fetchall()
        return dict(items=[json.loads(row[0]) for row in rows], total=total, offset=offset, limit=limit)

    def put(self, key, value, kind):
        result = "conflict" if kind == "conflict" else value["result"]
        item = value.get("item") or value.get("left") or {}
        search = " ".join(str(item.get(k) or "") for k in ("title", "series_title", "year", "season", "episode"))
        search += " " + str(value.get("key") or "") + " " + json.dumps(item.get("ids") or {}, ensure_ascii=False)
        selectable = bool(value.get("selectable")) and kind == "change"
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
        with self.lock:
            self.db.execute("INSERT INTO review(id,kind,feature,result,selectable,selected,search,payload) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,search=excluded.search",
                            (key, kind, value["feature"], result, selectable, selectable, search.casefold(), payload))

    def finish(self, previous=None):
        with self.lock:
            self.db.commit()
            if previous is not None:
                with previous.lock:
                    previous.db.commit()
                self.db.execute("ATTACH DATABASE ? AS previous", (str(previous.path),))
                try:
                    self.db.execute("UPDATE review SET selected=selectable AND EXISTS(SELECT 1 FROM previous.review old WHERE old.id=review.id AND old.selected=1)")
                    self.db.commit()
                finally:
                    self.db.execute("DETACH DATABASE previous")
            self.db.execute("CREATE INDEX IF NOT EXISTS review_filter ON review(feature,result,seq)")
            self.db.execute("CREATE INDEX IF NOT EXISTS review_result ON review(result,seq)")
            self.db.execute("CREATE INDEX IF NOT EXISTS review_selection ON review(selected,id)")
            self.db.commit()
            groups = self.db.execute("SELECT kind,result,COUNT(*),SUM(selected),SUM(selectable) FROM review GROUP BY kind,result").fetchall()
            self.counts = dict(changes=0, conflicts=0, attention=0, selected=0)
            self.selectable_count = 0
            for kind, result, count, selected, selectable in groups:
                self.counts["conflicts" if kind == "conflict" else "changes"] += count
                self.counts["selected"] += selected
                self.selectable_count += selectable
                if result in ("unresolved", "blocked"):
                    self.counts["attention"] += count
            self.features = [r[0] for r in self.db.execute("SELECT DISTINCT feature FROM review ORDER BY feature")]

    @staticmethod
    def where(feature="", result="", q=""):
        clauses, args = ["1=1"], []
        for name, value in (("feature", feature), ("result", result)):
            if value:
                clauses.append(f"{name}=?")
                args.append(value)
        if q:
            clauses.append("instr(search,?)>0")
            args.append(q.casefold())
        return " AND ".join(clauses), args

    def page(self, *, offset=0, limit=75, feature="", result="", q=""):
        where, args = self.where(feature, result, q)
        with self.lock:
            if not args:
                total = self.counts["changes"] + self.counts["conflicts"]
                selectable, selected = self.selectable_count, self.counts["selected"]
            else:
                total, selectable, selected = self.db.execute(f"SELECT COUNT(*),COALESCE(SUM(selectable),0),COALESCE(SUM(selected),0) FROM review WHERE {where}", args).fetchone()
            offset = min(offset, max(0, ((total - 1) // limit) * limit))
            rows = self.db.execute(f"SELECT payload,kind,selected FROM review WHERE {where} ORDER BY seq LIMIT ? OFFSET ?", (*args, limit, offset)).fetchall()
        items = []
        for payload, kind, chosen in rows:
            row = json.loads(payload)
            row["selected"] = bool(chosen)
            if kind == "conflict":
                row.update(result="conflict", item=row["left"], selectable=False)
            if row["feature"] == "playlists":
                for field in ("item", "before", "left", "right"):
                    if isinstance(row.get(field), dict):
                        for order in ("target_order", "current_order"):
                            values = row[field].pop(order, None)
                            if values is not None:
                                row[field][order + "_count"] = len(values)
            items.append(row)
        return dict(items=items, total=total, selectable=selectable, selected=selected, offset=offset, limit=limit)

    def select(self, selected, *, ids=None, feature="", result="", q=""):
        where, args = self.where(feature, result, q)
        with self.lock:
            if ids is not None:
                placeholders = ",".join("?" for _ in ids)
                found = self.db.execute(f"SELECT COUNT(*) FROM review WHERE selectable=1 AND id IN ({placeholders})", ids).fetchone()[0]
                if found != len(set(ids)):
                    raise ValueError("Select valid changes from this review")
                where += f" AND id IN ({placeholders})"
                args.extend(ids)
            changed = self.db.execute(f"UPDATE review SET selected=? WHERE selectable=1 AND selected!=? AND {where}", (selected, selected, *args)).rowcount
            self.db.commit()
            self.counts["selected"] += changed if selected else -changed

    def selected_ids(self):
        with self.lock:
            return {r[0] for r in self.db.execute("SELECT id FROM review WHERE selected=1")}

    def recheck_details(self, previous, missing, *, limit=5):
        from itertools import islice

        details = []
        identity = ("key", "source", "source_instance", "provider", "instance", "scope", "operation")
        with self.lock:
            for rid in islice(iter(missing), limit):
                old = previous.rows.get(rid)
                if old is None:
                    continue
                where = " AND ".join(f"json_extract(payload,'$.{field}') IS ?" for field in identity)
                rows = self.db.execute(f"SELECT payload FROM review WHERE kind='change' AND feature=? AND {where} LIMIT 2",
                                       (old["feature"], *(old.get(field) for field in identity))).fetchall()
                entry = dict(key=old["key"], feature=old["feature"], provider=old["provider"], instance=old["instance"],
                             operation=old["operation"], reason="unavailable" if not rows else "ambiguous", fields=[])
                if len(rows) == 1:
                    current = json.loads(rows[0][0])
                    changed = []

                    def compare(left, right, path):
                        if len(changed) >= 20:
                            return
                        if isinstance(left, dict) and isinstance(right, dict):
                            for field in sorted(left.keys() | right.keys()):
                                child = f"{path}.{field}" if path else field
                                if field not in left or field not in right:
                                    if len(changed) < 20:
                                        changed.append(child)
                                else:
                                    compare(left[field], right[field], child)
                        elif json.dumps(left, sort_keys=True) != json.dumps(right, sort_keys=True):
                            changed.append(path)

                    for field in ("item", "before", "destination_label", "selectable", "reason"):
                        compare(old.get(field), current.get(field), field)
                    entry.update(reason="changed", fields=changed)
                details.append(entry)
        return details

    def close(self):
        with self.lock:
            self.cleanup()

    @staticmethod
    def _cleanup(db, path):
        db.close()
        path.unlink(missing_ok=True)
