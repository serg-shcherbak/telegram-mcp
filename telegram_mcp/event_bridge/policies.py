"""Clock-driven settling and explicit evidence-linked conclusions; no Telegram I/O."""

from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .store import ObligationChange


def _quiet(policy, now):
    local = now.astimezone(ZoneInfo(policy.quiet_timezone))
    minute = local.hour * 60 + local.minute
    start, end = policy.quiet_start_minute, policy.quiet_end_minute
    return start is not None and (
        (start <= minute < end) if start < end else (minute >= start or minute < end)
    )


class PolicyScheduler:
    def __init__(
        self,
        store,
        *,
        owner_id,
        clock=lambda: datetime.now(timezone.utc),
        silent_runs_verified=False,
    ):
        self.store, self.owner_id, self.clock = store, owner_id, clock
        self.silent_runs_verified = silent_runs_verified

    def flush(self, watch_id, deliveries=()):
        watch, pending, last = self.store.policy_pending(self.owner_id, watch_id)
        if not watch or not pending:
            return None
        policy = watch.active.policy
        now = self.clock()
        incoming = [s for s in pending if not s.record.metadata.get("outgoing", False)]
        if incoming:
            if _quiet(policy, now):
                return None
            if (now - pending[-1].record.occurred_at).total_seconds() < policy.settle_seconds and (
                now - incoming[0].record.occurred_at
            ).total_seconds() < policy.max_settle_seconds:
                return None
            if last and (now - last).total_seconds() < policy.min_wake_seconds:
                return None
        # Until the host proves independent silent runs, every packet can be visible.
        conservative = not self.silent_runs_verified or policy.notify
        watch_limit = (
            min(policy.wakes_per_hour, policy.notifications_per_hour)
            if conservative
            else policy.wakes_per_hour
        )
        account_limit = (
            min(policy.account_wakes_per_hour, policy.account_notifications_per_hour)
            if conservative
            else policy.account_wakes_per_hour
        )
        return self.store.dispatch_policy(
            self.owner_id,
            watch_id,
            watch.epoch,
            tuple(s.sequence for s in pending),
            deliveries=deliveries,
            now=now,
            watch_limit=watch_limit,
            account_limit=account_limit,
        )


@dataclass(frozen=True)
class AnalysisResult:
    obligation_id: str
    state: str
    summary: str
    evidence: tuple
    confidence: float
    due_at: str = None
    due_origin: str = None

    def __post_init__(self):
        if type(self.confidence) not in (int, float) or not 0 <= self.confidence <= 1:
            raise ValueError("Confidence must be in 0..1")
        if not self.evidence:
            raise ValueError("An interpretation requires source evidence")
        if self.due_at is not None:
            due = datetime.fromisoformat(self.due_at)
            if due.tzinfo is None or self.due_origin not in ("user", "message"):
                raise ValueError("An aware deadline and explicit provenance are required")
        elif self.due_origin is not None:
            raise ValueError("Deadline provenance needs a deadline")


def apply_analysis(store, snapshot, results):
    changes = tuple(
        ObligationChange(
            r.obligation_id,
            r.state if r.confidence >= 0.8 else "needs_review",
            r.summary,
            r.evidence,
            confidence=r.confidence,
            due_at=r.due_at,
            due_origin=r.due_origin,
        )
        for r in results
    )
    return store.commit_analysis(snapshot, changes)


@dataclass(frozen=True)
class ReminderRecheck:
    job_id: str
    obligation_id: str
    due_at: str
    snapshot: object


class ReminderPolicy:
    """Explicit one-shot rules return analysis work, never a confirmed user alert.

    The caller re-reads the scoped conversation, commits with the pinned snapshot,
    then acknowledges this exact rule. A fresh source invalidates the old job.
    """

    def __init__(self, store, *, owner_id, clock=lambda: datetime.now(timezone.utc)):
        self.store, self.owner_id, self.clock = store, owner_id, clock

    def schedule(self, watch_id, obligation_id, *, expected_version, due_at):
        return self.store.schedule_reminder(
            self.owner_id, watch_id, obligation_id, expected_version, due_at
        )

    def due(self, watch_id):
        from hashlib import sha256

        watch = self.store.get(self.owner_id, watch_id)
        if not watch or not watch.active or _quiet(watch.active.policy, self.clock()):
            return ()
        return tuple(
            ReminderRecheck(
                sha256(repr((snapshot, due)).encode()).hexdigest(), obligation, due, snapshot
            )
            for obligation, due, snapshot in self.store.due_reminders(
                self.owner_id, watch_id, now=self.clock()
            )
        )

    def complete(self, job):
        return self.store.complete_reminder(job.snapshot, job.obligation_id, job.due_at)
