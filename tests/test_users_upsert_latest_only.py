from er_stats.db import parse_start_time


def _row_for(store, uid: str):
    cur = store.connection.execute(
        """
        SELECT uid, nickname, first_seen, last_seen, ingested_until, last_mmr, last_language, ml_bot
        FROM users WHERE uid=?
        """,
        (uid,),
    )
    return cur.fetchone()


def test_upsert_user_updates_only_when_newer(store):
    uid = "uid-777"

    older_ts = "2025-01-01T00:00:00.000+0000"
    newer_ts = "2025-02-01T00:00:00.000+0000"

    # Seed with older record
    store.upsert_user(
        {
            "uid": uid,
            "nickname": "oldnick",
            "startDtm": older_ts,
            "mmrAfter": 100,
            "language": "en",
            "mlbot": False,
        }
    )

    r1 = _row_for(store, uid)
    assert r1 is not None
    assert r1[0] == uid
    assert r1[1] == "oldnick"
    assert r1[2] == parse_start_time(older_ts)  # first_seen
    assert r1[3] == parse_start_time(older_ts)  # last_seen
    assert r1[4] == parse_start_time(older_ts)  # ingested_until
    assert r1[5] == 100  # last_mmr

    # Try to apply an older payload again with different values; it must NOT update
    store.upsert_user(
        {
            "uid": uid,
            "nickname": "should_not_apply",
            "startDtm": "2024-12-31T23:59:59.000+0000",
            "mmrAfter": 999,
            "language": "jp",
            "mlbot": True,
        }
    )

    r2 = _row_for(store, uid)
    assert r2 == r1  # unchanged

    # Apply a newer payload; it SHOULD update nickname/last_seen/mmr/lang/ml_bot
    store.upsert_user(
        {
            "uid": uid,
            "nickname": "newnick",
            "startDtm": newer_ts,
            "mmrAfter": 200,
            "language": "ko",
            "mlbot": True,
        }
    )

    r3 = _row_for(store, uid)
    assert r3[0] == uid
    assert r3[1] == "newnick"
    # first_seen should remain the older timestamp
    assert r3[2] == parse_start_time(older_ts)
    # last_seen should be updated to the newer timestamp
    assert r3[3] == parse_start_time(newer_ts)
    assert r3[4] == parse_start_time(newer_ts)
    assert r3[5] == 200
    assert r3[6] == "ko"
    assert r3[7] == 1  # ml_bot stored as integer


def test_upsert_user_new_then_older_does_not_downgrade(store):
    uid = "uid-778"

    newer_ts = "2025-03-01T12:00:00.000+0000"
    older_ts = "2025-02-01T12:00:00.000+0000"

    # Insert newer first
    store.upsert_user(
        {
            "uid": uid,
            "nickname": "nickA",
            "startDtm": newer_ts,
            "mmrAfter": 500,
            "language": "en",
            "mlbot": False,
        }
    )

    r1 = _row_for(store, uid)

    # Then try to "downgrade" with an older snapshot; should be ignored
    store.upsert_user(
        {
            "uid": uid,
            "nickname": "nickB",
            "startDtm": older_ts,
            "mmrAfter": 50,
            "language": "jp",
            "mlbot": True,
        }
    )

    r2 = _row_for(store, uid)
    assert r2 == r1  # unchanged


def test_upsert_user_does_not_advance_ingested_when_flag_false(store):
    uid = "uid-779"

    initial_ts = "2025-04-01T00:00:00.000+0000"
    later_ts = "2025-05-01T00:00:00.000+0000"

    store.upsert_user(
        {
            "uid": uid,
            "nickname": "observer",
            "startDtm": initial_ts,
            "mmrAfter": 10,
            "language": "en",
            "mlbot": False,
        },
        mark_ingested=False,
    )

    observed = _row_for(store, uid)
    assert observed is not None
    assert observed[3] == parse_start_time(initial_ts)
    assert observed[4] is None

    store.upsert_user(
        {
            "uid": uid,
            "nickname": "observer",
            "startDtm": later_ts,
            "mmrAfter": 20,
            "language": "en",
            "mlbot": False,
        }
    )

    ingested = _row_for(store, uid)
    assert ingested[4] == parse_start_time(later_ts)
