from er_stats.db import parse_start_time


def _row_for(store, user_num: int):
    cur = store.connection.execute(
        "SELECT user_num, nickname, first_seen, last_seen, last_mmr, last_language, ml_bot FROM users WHERE user_num=?",
        (user_num,),
    )
    return cur.fetchone()


def test_upsert_user_updates_only_when_newer(store):
    user_num = 777

    older_ts = "2025-01-01T00:00:00.000+0000"
    newer_ts = "2025-02-01T00:00:00.000+0000"

    # Seed with older record
    store.upsert_user({
        "userNum": user_num,
        "nickname": "oldnick",
        "startDtm": older_ts,
        "mmrAfter": 100,
        "language": "en",
        "mlbot": False,
    })

    r1 = _row_for(store, user_num)
    assert r1 is not None
    assert r1[0] == user_num
    assert r1[1] == "oldnick"
    assert r1[2] == parse_start_time(older_ts)  # first_seen
    assert r1[3] == parse_start_time(older_ts)  # last_seen
    assert r1[4] == 100  # last_mmr

    # Try to apply an older payload again with different values; it must NOT update
    store.upsert_user({
        "userNum": user_num,
        "nickname": "should_not_apply",
        "startDtm": "2024-12-31T23:59:59.000+0000",
        "mmrAfter": 999,
        "language": "jp",
        "mlbot": True,
    })

    r2 = _row_for(store, user_num)
    assert r2 == r1  # unchanged

    # Apply a newer payload; it SHOULD update nickname/last_seen/mmr/lang/ml_bot
    store.upsert_user({
        "userNum": user_num,
        "nickname": "newnick",
        "startDtm": newer_ts,
        "mmrAfter": 200,
        "language": "ko",
        "mlbot": True,
    })

    r3 = _row_for(store, user_num)
    assert r3[0] == user_num
    assert r3[1] == "newnick"
    # first_seen should remain the older timestamp
    assert r3[2] == parse_start_time(older_ts)
    # last_seen should be updated to the newer timestamp
    assert r3[3] == parse_start_time(newer_ts)
    assert r3[4] == 200
    assert r3[5] == "ko"
    assert r3[6] == 1  # ml_bot stored as integer


def test_upsert_user_new_then_older_does_not_downgrade(store):
    user_num = 778

    newer_ts = "2025-03-01T12:00:00.000+0000"
    older_ts = "2025-02-01T12:00:00.000+0000"

    # Insert newer first
    store.upsert_user({
        "userNum": user_num,
        "nickname": "nickA",
        "startDtm": newer_ts,
        "mmrAfter": 500,
        "language": "en",
        "mlbot": False,
    })

    r1 = _row_for(store, user_num)

    # Then try to "downgrade" with an older snapshot; should be ignored
    store.upsert_user({
        "userNum": user_num,
        "nickname": "nickB",
        "startDtm": older_ts,
        "mmrAfter": 50,
        "language": "jp",
        "mlbot": True,
    })

    r2 = _row_for(store, user_num)
    assert r2 == r1  # unchanged

