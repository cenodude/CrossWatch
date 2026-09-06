# services/interactive_sync_report.py
# CrossWatch - Interactive Sync Completion Reports
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timezone
import json
import time
from cw_platform.orchestrator._interactive import fingerprint


COUNTERS = ("added", "updated", "removed", "skipped", "unresolved", "blocked", "errors")


def count(value):
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def timestamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds")


class SyncReport:
    def __init__(self, store=None, pair=None):
        self.started = time.time()
        self.features = []
        self.notices = {}
        self.notice_overflow = 0
        self.cancelled = False
        self.store = store
        self.pair = pair or {}
        self.operations = {}
        self.authoritative = set()
        self.detail_omitted = {}
        if store is not None:
            store.clear_report_issues()

    def item_event(self, value):
        if self.store is None:
            return
        name = value.get("event")
        provider = str(value.get("provider") or value.get("dst") or "")[:160]
        feature = str(value.get("feature") or "")[:32]
        if name in ("apply:add:start", "apply:update:start", "apply:remove:start"):
            self.operations[(provider, feature)] = name.split(":")[1]
            return
        if name not in ("apply:unresolved", "archive:item_failures"):
            return
        operation = str(value.get("op") or self.operations.get((provider, feature), "add"))
        scope = (provider, feature, operation)
        provisional = name == "apply:unresolved"
        if provisional and scope in self.authoritative:
            return
        if not provisional:
            self.authoritative.add(scope)
            self.store.clear_report_issues(*scope)
            self.detail_omitted.pop(scope, None)
        else:
            self.detail_omitted[scope] = self.detail_omitted.get(scope, 0) + count(value.get("omitted"))
        instances = {self.pair.get(side + "_instance") or "default" for side in ("source", "target") if self.pair.get(side) == provider}
        instance = next(iter(instances)) if len(instances) == 1 else ""
        for raw in value.get("items") or []:
            if not isinstance(raw, dict):
                continue
            item = raw.get("item")
            if not isinstance(item, dict):
                item = raw
            safe_item: dict[str, object] = {key: str(item[key])[:500] for key in ("title", "name", "type", "year", "season", "episode", "series_title", "show_title") if item.get(key) is not None}
            ids = item.get("ids") or {}
            safe_item["ids"] = {key: str(ids[key])[:128] for key in ("imdb", "tmdb", "tvdb", "trakt", "simkl", "mal", "anilist") if isinstance(ids, dict) and ids.get(key)}
            reason = str(raw.get("reason") or raw.get("hint") or "The provider did not confirm this change.")[:2000]
            explanation = {
                "not_found": "The destination provider could not find this item.",
                "missing_write_ids": "The item is missing IDs required by the destination provider.",
                "missing_auth": "The destination provider requires authentication.",
                "rate_limited": "The provider's request limit was reached.",
                "apply:add:failed": "The provider did not confirm that this item was added.",
                "apply:add:unaccounted": "The provider response did not account for this item.",
            }.get(reason, "")
            key = str(raw.get("key") or "")[:500]
            row = dict(provider=provider, instance=instance, feature=feature, operation=operation,
                       result="blocked" if raw.get("promoted") else "unresolved" if provisional else "failed",
                       key=key, item=safe_item, reason=reason, explanation=explanation)
            self.store.put_report_issue(fingerprint([scope, key or safe_item]), row, provisional)

    def event(self, value):
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (ValueError, TypeError):
                return
        if not isinstance(value, dict):
            return
        name = str(value.get("event") or "")
        self.item_event(value)
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
        return dict(version=2, run_id="interactive-" + session.id, pair=dict(session.pair),
                    started_at=timestamp(self.started), finished_at=timestamp(time.time()),
                    duration_seconds=session.progress.public().get("elapsed_seconds", 0),
                    outcome=outcome, totals=totals, requested=requested,
                    proposed=count(counts.get("changes")),
                    not_selected=max(0, count(counts.get("changes")) - requested),
                    reached_execution=requested - not_reached, not_reached=not_reached,
                    conflicts_reviewed=count(counts.get("conflicts")),
                    features=self.features, notices=list(self.notices.values()), notice_overflow=self.notice_overflow,
                    issue_count=self.store.report_page(limit=1)["total"] if self.store is not None else 0,
                    issue_details_omitted=sum(self.detail_omitted.values()),
                    review_notices=session.plan.notices[:100],
                    requests=session.progress.public().get("requests", 0),
                    accounting_note="Totals use the existing sync engine's provider results. Skips can include items already present. Errors and protection counts can overlap or describe a whole batch; they are not an item-by-item receipt. Playlist totals count playlist contents and ordering operations.",
                    incomplete_note="Some writes may have completed before the interruption without final counts. Check Events before starting another sync." if outcome in ("cancelled", "incomplete") else "")
