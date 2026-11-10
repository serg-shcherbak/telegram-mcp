from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import json
from uuid import uuid4

from .scope import ScopeDenied
from .store import AnalysisSnapshot
from .policies import apply_analysis


class AnalysisCoordinator:
    def __init__(self, reader, profile_id, *, clock=lambda: datetime.now(timezone.utc)):
        if not profile_id:
            raise ValueError("Stable trusted profile ID required")
        self.reader, self.store, self.profile_id, self.clock = (
            reader,
            reader.store,
            profile_id,
            clock,
        )

    def sources(self, watch_id, after=0):
        watch = self.reader._watch(watch_id)
        if type(after) is not int or after < 0:
            raise ValueError("Nonnegative cursor required")
        with self.store._transaction() as db:
            ids = [
                r[0]
                for r in db.execute(
                    "SELECT seq FROM source_events WHERE owner=? AND watch=? AND epoch=? "
                    "AND seq>? ORDER BY seq LIMIT 101",
                    (watch.owner_id, watch_id, watch.epoch, after),
                )
            ]
        rows = [self.reader._source(watch, seq) for seq in ids]
        self.reader._watch(watch_id, watch.epoch)
        return {
            "sources": [
                {
                    "sequence": r.sequence,
                    "epoch": r.epoch,
                    "target": asdict(r.record.target),
                    "message_id": r.record.message_id,
                    "kind": r.record.kind,
                }
                for r in rows[:100]
            ],
            "has_more": len(rows) > 100,
        }

    async def begin(self, watch_id, source_sequences, obligation_ids=()):
        watch = self.reader._watch(watch_id)
        sequences = tuple(source_sequences)
        if not sequences or len(sequences) > 100 or len(set(sequences)) != len(sequences):
            raise ValueError("One to 100 distinct sources required")
        for sequence in sequences:
            self.reader._source(watch, sequence)
        snapshot = self.store.analysis_snapshot(
            watch.owner_id, watch_id, watch.epoch, obligation_ids
        )
        now = self.clock()
        job = uuid4().hex
        document = json.dumps(
            {"snapshot": asdict(snapshot), "sources": sequences}, ensure_ascii=False
        )
        with self.store._transaction() as db:
            db.execute(
                "DELETE FROM analysis_jobs WHERE owner=? AND expires<=?",
                (watch.owner_id, now.isoformat()),
            )
            if (
                db.execute(
                    "SELECT COUNT(*) FROM analysis_jobs WHERE owner=?", (watch.owner_id,)
                ).fetchone()[0]
                >= 100
            ):
                raise ValueError("Analysis job capacity reached")
            db.execute(
                "INSERT INTO analysis_jobs VALUES(?,?,?,?,?,?,NULL)",
                (
                    job,
                    watch.owner_id,
                    self.profile_id,
                    watch_id,
                    (now + timedelta(minutes=15)).isoformat(),
                    document,
                ),
            )
        context = await self.reader.read(watch_id, sequences)
        return {
            "job_id": job,
            "context": asdict(context),
            "expires_in_seconds": 900,
            "policy": asdict(watch.active.policy),
        }

    def _job(self, db, job_id):
        row = db.execute(
            "SELECT * FROM analysis_jobs WHERE job=? AND owner=? AND profile=? AND expires>?",
            (job_id, self.reader.profile.owner_id, self.profile_id, self.clock().isoformat()),
        ).fetchone()
        if row is None:
            raise ScopeDenied("Current owned analysis job required")
        if row["watch"] not in self.reader.profile.allowed_watch_ids:
            raise ScopeDenied("Watch no longer authorized")
        self.store._active(
            db,
            self.reader.profile.owner_id,
            row["watch"],
            json.loads(row["document"])["snapshot"]["epoch"],
        )
        return row

    def _receipt(self, db, row):
        receipt = json.loads(row["receipt"])
        document = json.loads(row["document"])
        snapshot = document["snapshot"]
        evidence_current = all(
            db.execute(
                "SELECT 1 FROM source_events WHERE owner=? AND watch=? AND epoch=? AND seq=?",
                (row["owner"], row["watch"], snapshot["epoch"], seq),
            ).fetchone()
            for seq in document["sources"]
        )
        watermark = db.execute(
            "SELECT watermark FROM watches WHERE owner=? AND watch=?", (row["owner"], row["watch"])
        ).fetchone()[0]
        versions = receipt.get("obligation_versions", {})
        actual = self.store._obligation_versions(db, row["owner"], row["watch"], versions)
        receipt["current"] = bool(
            receipt["analysis_completed"]
            and evidence_current
            and watermark == snapshot["watermark"]
            and all(actual.get(k, 0) == v for k, v in versions.items())
        )
        return receipt

    async def commit(self, job_id, results):
        with self.store._transaction() as db:
            row = self._job(db, job_id)
            if row["receipt"]:
                return self._receipt(db, row)
            data = json.loads(row["document"])
        context = await self.reader.read(row["watch"], data["sources"])
        if any(m.gap or m.truncated for m in context.messages):
            from dataclasses import replace

            results = tuple(
                replace(r, confidence=min(r.confidence, 0.79), due_at=None, due_origin=None)
                for r in results
            )
        snapshot_data = data["snapshot"]
        snapshot_data["obligation_versions"] = tuple(
            tuple(v) for v in snapshot_data["obligation_versions"]
        )
        snapshot = AnalysisSnapshot(**snapshot_data)
        with self.store._transaction() as db:
            row = self._job(db, job_id)
            if row["receipt"]:
                return self._receipt(db, row)

            class Commit:
                def commit_analysis(_, supplied, changes):
                    expected = dict(supplied.obligation_versions)
                    if len({c.obligation_id for c in changes}) != len(changes) or any(
                        c.obligation_id not in expected for c in changes
                    ):
                        raise ValueError("All obligations must be pinned once")
                    if any(seq not in data["sources"] for c in changes for seq in c.evidence):
                        raise ScopeDenied("Evidence must have been read by this job")
                    return self.store._commit_analysis(db, supplied, changes)

            success = apply_analysis(Commit(), snapshot, results)
            if success and not any(m.gap or m.truncated for m in context.messages):
                for result in results:
                    db.execute(
                        "UPDATE reminder_rules SET fired=1 WHERE owner=? AND watch=? "
                        "AND obligation=? AND epoch=? AND version=? AND watermark=? AND due<=?",
                        (
                            snapshot.owner_id,
                            snapshot.watch_id,
                            result.obligation_id,
                            snapshot.epoch,
                            dict(snapshot.obligation_versions)[result.obligation_id],
                            snapshot.watermark,
                            self.clock().isoformat(),
                        ),
                    )
            receipt = {
                "job_id": job_id,
                "analysis_completed": success,
                "event_delivery_status": "not_checked",
                "current": success,
                "obligation_versions": {
                    key: version + int(any(r.obligation_id == key for r in results))
                    for key, version in snapshot.obligation_versions
                },
            }
            db.execute(
                "UPDATE analysis_jobs SET receipt=? WHERE job=?",
                (json.dumps(receipt, ensure_ascii=False), job_id),
            )
            return receipt


def register_analysis_tools(mcp, coordinator):
    from mcp.types import ToolAnnotations
    from .policies import AnalysisResult

    read = ToolAnnotations(readOnlyHint=True, openWorldHint=True)
    write = ToolAnnotations(readOnlyHint=False, destructiveHint=False, openWorldHint=True)

    @mcp.tool(annotations=read)
    def event_sources(watch_id: str, after: int = 0) -> dict:
        return coordinator.sources(watch_id, after)

    @mcp.tool(annotations=read)
    async def event_context(watch_id: str, source_sequences: list[int]) -> dict:
        return asdict(await coordinator.reader.read(watch_id, source_sequences))

    @mcp.tool(annotations=read)
    def event_obligations(watch_id: str) -> dict:
        return asdict(coordinator.reader.get_obligations(watch_id))

    @mcp.tool(annotations=write)
    async def begin_analysis(
        watch_id: str, source_sequences: list[int], obligation_ids: list[str]
    ) -> dict:
        return await coordinator.begin(watch_id, source_sequences, obligation_ids)

    @mcp.tool(annotations=write)
    async def commit_analysis(job_id: str, results: list[dict]) -> dict:
        if len(results) > 100:
            raise ValueError("At most 100 conclusions")
        return await coordinator.commit(job_id, tuple(AnalysisResult(**r) for r in results))
