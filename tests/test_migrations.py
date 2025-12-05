import sqlite3

from er_stats.db import SQLiteStore, parse_start_time


def test_setup_schema_preserves_null_ingested_until(tmp_path):
    db_path = tmp_path / "legacy.db"

    # Simulate legacy schema without ingested_until
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE users (
            uid TEXT PRIMARY KEY,
            nickname TEXT,
            first_seen TEXT,
            last_seen TEXT,
            last_checked TEXT,
            last_mmr INTEGER,
            ml_bot INTEGER DEFAULT 0,
            last_language TEXT,
            deleted INTEGER DEFAULT 0
        )
        """
    )
    legacy_ts = "2025-01-01T00:00:00.000+0000"
    conn.execute(
        """
        INSERT INTO users (
            uid, nickname, first_seen, last_seen, last_checked, last_mmr, ml_bot, last_language, deleted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "legacy-uid",
            "legacy",
            parse_start_time(legacy_ts),
            parse_start_time(legacy_ts),
            parse_start_time(legacy_ts),
            10,
            0,
            "en",
            0,
        ),
    )
    conn.commit()
    conn.close()

    store = SQLiteStore(str(db_path))
    store.setup_schema()

    row = store.connection.execute(
        "SELECT last_seen, ingested_until FROM users WHERE uid=?", ("legacy-uid",)
    ).fetchone()

    assert row["last_seen"] == parse_start_time(legacy_ts)
    assert row["ingested_until"] is None

    # Ensure subsequent setup runs do not overwrite intentionally null ingested_until values
    observed_only_ts = "2025-02-01T00:00:00.000+0000"
    store.upsert_user(
        {
            "uid": "observer",
            "nickname": "observer",
            "startDtm": observed_only_ts,
            "mmrAfter": 20,
            "language": "en",
        },
        mark_ingested=False,
    )

    store.setup_schema()

    observer_row = store.connection.execute(
        "SELECT ingested_until FROM users WHERE uid=?", ("observer",)
    ).fetchone()

    assert observer_row["ingested_until"] is None
