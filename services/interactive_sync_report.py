# services/interactive_sync_report.py
# CrossWatch - Interactive Sync Completion Reports
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timezone
import json
import time


COUNTERS = ("added", "updated", "removed", "skipped", "unresolved", "blocked", "errors")


def count(value):
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def timestamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds")


class SyncReport:
    def __init__(self):
        self.started = time.time()
        self.features = []
        self.notices = {}
        self.notice_overflow = 0
        self.cancelled = False

    def event(self, value):
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (ValueError, TypeError):
                return
        if not isinstance(value, dict):
            return
        name = str(value.get("event") or "")
        if name in ("feature:cancelled", "run:cancelled"):
            self.cancelled = True
        reasons = {
            "feature:error": "A feature could not finish. Check Events for the provider error.",
            "feature:unsupported": "A provider does not support this feature or is unavailable.",
            "playlist:mapping:error": "A playlist mapping could not finish. Check Events for details.",
            "writes:skipped": "Writes were skipped by the sync engine.",
            "pair:skip": "The pair was skipped by the sync engine.",
            "run:pair:skip": "The pair was skipped by the sync engine.",
        }
        reason = reasons.get(name)
        if "mass_delete" in name or "massdelete" in name:
            reason = "Mass delete protection was triggered. Check Events for details."
        if not reason:
            return
        feature = str(value.get("feature") or "")[:32]
        provider = str(value.get("dst") or value.get("provider") or "")[:80]
        key = (name, feature, provider)
        if key in self.notices:
            self.notices[key]["occurrences"] += 1
        elif len(self.notices) < 100:
            self.notices[key] = dict(event=name, feature=feature, provider=provider, reason=reason, occurrences=1)
        else:
            self.notice_overflow += 1

    def record(self, feature, src, dst, src_instance, dst_instance, mode, result):
        totals = {key: count(result.get(key)) for key in COUNTERS}
        destinations = []
        if mode == "two-way" and feature != "playlists":
            for side, provider, instance in (("A", src, src_instance), ("B", dst, dst_instance)):
                row = dict(provider=provider, instance=instance,
                           added=count(result.get(f"adds_to_{side}")),
                           updated=count(result.get(f"upd_to_{side}")),
                           removed=count(result.get(f"rem_from_{side}")),
                           unresolved=count(result.get(f"unresolved_to_{side}")))
                for key in ("skipped", "errors"):
                    row[key] = sum(count((result.get(f"res{side}_{op}") or {}).get(key)) for op in ("add", "update", "remove"))
                destinations.append(row)
            for key in ("added", "updated", "removed"):
                totals[key] = sum(row[key] for row in destinations)
            totals["unresolved"] = count(result.get("unresolved", sum(row["unresolved"] for row in destinations)))
            for key in ("skipped", "errors"):
                totals[key] += count(result.get(f"{key}_to_A")) + count(result.get(f"{key}_to_B"))
        elif feature != "playlists":
            destinations.append(dict(provider=dst, instance=dst_instance, **totals))
        self.features.append(dict(feature=feature, **totals, destinations=destinations))

    def finish(self, session, execution, result):
        totals = {key: count(result.get(key)) if result is not None else sum(row[key] for row in self.features) for key in COUNTERS}
        requested = len(execution.selected)
        not_reached = len(execution.selected - execution.seen)
        cancelled = self.cancelled or bool((result or {}).get("cancelled"))
        outcome = "cancelled" if cancelled else "incomplete" if result is None or not result.get("ok", True) else "attention" if not_reached or any(totals[k] for k in ("errors", "unresolved", "blocked")) or self.notices else "success"
        counts = session.store.counts if session.store else {}
        return dict(version=1, run_id="interactive-" + session.id, pair=dict(session.pair),
                    started_at=timestamp(self.started), finished_at=timestamp(time.time()),
                    duration_seconds=session.progress.public().get("elapsed_seconds", 0),
                    outcome=outcome, totals=totals, requested=requested,
                    proposed=count(counts.get("changes")),
                    not_selected=max(0, count(counts.get("changes")) - requested),
                    reached_execution=requested - not_reached, not_reached=not_reached,
                    conflicts_reviewed=count(counts.get("conflicts")),
                    features=self.features, notices=list(self.notices.values()), notice_overflow=self.notice_overflow,
                    review_notices=session.plan.notices[:100],
                    requests=session.progress.public().get("requests", 0),
                    accounting_note="Totals use the existing sync engine's provider results. Skips can include items already present. Errors and protection counts can overlap or describe a whole batch; they are not an item-by-item receipt. Playlist totals count playlist contents and ordering operations.",
                    incomplete_note="Some writes may have completed before the interruption without final counts. Check Events before starting another sync." if outcome in ("cancelled", "incomplete") else "")
