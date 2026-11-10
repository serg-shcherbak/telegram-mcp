import sqlite3

import pytest

from telegram_mcp import transcription


@pytest.mark.parametrize("legacy", [False, True])
def test_cache_retention_and_migration_without_unixepoch(
    transcript_cache_dir, monkeypatch, legacy
):
    path = transcript_cache_dir / "transcripts.db"
    if legacy:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with sqlite3.connect(path) as connection:
            connection.execute(
                "CREATE TABLE transcripts (chat_id INTEGER, message_id INTEGER, source TEXT, "
                "text TEXT, duration INTEGER, lang TEXT, created_at TEXT, "
                "PRIMARY KEY(chat_id, message_id))"
            )
            connection.execute(
                "INSERT INTO transcripts VALUES (1, 2, 'openai', 'legacy text', 3, 'en', "
                "datetime('now'))"
            )

    connect = sqlite3.connect

    def compatible_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_authorizer(
            lambda action, arg1, arg2, database, origin: (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_FUNCTION and arg2 == "unixepoch"
                else sqlite3.SQLITE_OK
            )
        )
        return connection

    monkeypatch.setattr(transcription.sqlite3, "connect", compatible_connect)
    if legacy:
        assert transcription.get_cached_transcript(1, 2, "openai")["text"] == "legacy text"
    transcription.save_transcript(1, 2, "openai", "current text")
    assert transcription.get_cached_transcript(1, 2, "openai")["text"] == "current text"
    now = transcription.time.time()
    monkeypatch.setenv("TELEGRAM_TRANSCRIPT_RETENTION_SECONDS", "1")
    monkeypatch.setattr(transcription.time, "time", lambda: now + 2)
    assert transcription.get_cached_transcript(1, 2, "openai") is None
