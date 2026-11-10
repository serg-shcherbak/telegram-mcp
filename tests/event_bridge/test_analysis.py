import asyncio
from dataclasses import replace
from datetime import timedelta
import pytest
from telegram_mcp.event_bridge.analysis import AnalysisCoordinator, register_analysis_tools
from telegram_mcp.event_bridge.context import ContextProfile, ScopedContextReader
from telegram_mcp.event_bridge.policies import AnalysisResult
from telegram_mcp.event_bridge.scope import ScopeDenied
from telegram_mcp.event_bridge.store import SQLiteStore
from test_store import active, record, NOW
from test_context import Client, message


def setup(tmp_path):
    store = SQLiteStore(tmp_path / "db")
    manager, watch = active(store)
    source = store.append_source("owner", "watch", watch.epoch, record())
    client = Client({1: message()})
    reader = ScopedContextReader(store, ContextProfile("owner", ("watch",)), {"account": client})
    coordinator = AnalysisCoordinator(reader, "context", clock=lambda: NOW)
    return store, manager, watch, source, client, coordinator


def test_persistent_job_retry_and_profile_fence(tmp_path):
    store, _, _, source, client, c = setup(tmp_path)
    job = asyncio.run(c.begin("watch", [source.sequence], ["task"]))
    assert job["policy"]["notify"] is False
    result = AnalysisResult("task", "awaiting_me", "question", (source.sequence,), 0.4)
    receipt = asyncio.run(c.commit(job["job_id"], (result,)))
    assert receipt["analysis_completed"] and receipt["event_delivery_status"] == "not_checked"
    assert store.get_obligations("owner", "watch")[0].state == "needs_review"
    assert asyncio.run(c.commit(job["job_id"], (result,))) == receipt
    foreign = AnalysisCoordinator(c.reader, "other", clock=lambda: NOW)
    with pytest.raises(ScopeDenied):
        asyncio.run(foreign.commit(job["job_id"], (result,)))
    c.clock = lambda: NOW + timedelta(minutes=16)
    with pytest.raises(ScopeDenied):
        asyncio.run(c.commit(job["job_id"], (result,)))
    store.close()


def test_job_cleanup_does_not_delete_another_owners_jobs(tmp_path):
    store, _, watch, source, _, coordinator = setup(tmp_path)
    try:
        assert store.compare_and_swap("other", "watch", None, replace(watch, owner_id="other"))
        with store._transaction() as db:
            db.execute(
                "INSERT INTO analysis_jobs VALUES(?,?,?,?,?,?,NULL)",
                (
                    "foreign-job",
                    "other",
                    "profile",
                    "watch",
                    (NOW - timedelta(minutes=1)).isoformat(),
                    "{}",
                ),
            )
        asyncio.run(coordinator.begin("watch", [source.sequence], ["task"]))
        with store._transaction() as db:
            assert db.execute("SELECT 1 FROM analysis_jobs WHERE job='foreign-job'").fetchone()
    finally:
        store.close()


@pytest.mark.parametrize("change", ["outgoing", "manual", "pause"])
def test_concurrent_change_defeats_job(tmp_path, change):
    store, manager, watch, source, client, c = setup(tmp_path)
    job = asyncio.run(c.begin("watch", [source.sequence], ["task"]))
    result = AnalysisResult("task", "awaiting_me", "question", (source.sequence,), 1)
    if change == "outgoing":
        store.append_source("owner", "watch", watch.epoch, record(2, outgoing=True))
    elif change == "manual":
        from telegram_mcp.event_bridge.store import ObligationChange

        snapshot = store.analysis_snapshot("owner", "watch", watch.epoch, ("task",))
        store.commit_analysis(
            snapshot, (ObligationChange("task", "done", "closed", (source.sequence,)),)
        )
    else:
        store.compare_and_swap(
            "owner",
            "watch",
            watch.version,
            replace(
                watch,
                version=watch.version + 1,
                epoch=watch.epoch + 1,
                state=__import__(
                    "telegram_mcp.event_bridge.models", fromlist=["WatchState"]
                ).WatchState.PAUSED,
            ),
        )
    if change == "pause":
        with pytest.raises(ScopeDenied):
            asyncio.run(c.commit(job["job_id"], (result,)))
    else:
        assert not asyncio.run(c.commit(job["job_id"], (result,)))["analysis_completed"]
    store.close()


def test_tools_real_fastmcp_annotations(tmp_path):
    from mcp.server.fastmcp import FastMCP

    store, _, _, _, _, c = setup(tmp_path)
    mcp = FastMCP()
    register_analysis_tools(mcp, c)
    tools = {t.name: t for t in asyncio.run(mcp.list_tools())}
    assert not tools["commit_analysis"].annotations.readOnlyHint
    assert "get_messages" not in tools and "send_message" not in tools
    store.close()


def test_source_metadata_and_cached_receipt_freshness(tmp_path):
    store, _, watch, source, client, c = setup(tmp_path)
    assert c.sources("watch")["sources"][0]["sequence"] == source.sequence
    job = asyncio.run(c.begin("watch", [source.sequence], ["task"]))
    result = AnalysisResult("task", "awaiting_me", "question", (source.sequence,), 1)
    first = asyncio.run(c.commit(job["job_id"], (result,)))
    assert first["current"]
    store.append_source("owner", "watch", watch.epoch, record(2, outgoing=True))
    retry = asyncio.run(c.commit(job["job_id"], (result,)))
    assert retry["analysis_completed"] and not retry["current"]
    store.close()


def test_missing_context_preserves_uncertainty(tmp_path):
    store, _, _, source, client, c = setup(tmp_path)
    job = asyncio.run(c.begin("watch", [source.sequence], ["task"]))
    client.messages.clear()
    result = AnalysisResult("task", "done", "uncertain", (source.sequence,), 1)
    assert asyncio.run(c.commit(job["job_id"], (result,)))["analysis_completed"]
    assert store.get_obligations("owner", "watch")[0].state == "needs_review"
    store.close()
