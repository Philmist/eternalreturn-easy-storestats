from er_stats.db import parse_start_time


def test_parse_start_time_variants():
    assert parse_start_time("2025-10-27T23:24:03.003+0900").startswith("2025-10-27T23:24:03")
    assert parse_start_time("2025-10-27T23:24:03+00:00").startswith("2025-10-27T23:24:03")
    assert parse_start_time("2025-10-27T23:24:03Z").endswith("+00:00")
    assert parse_start_time(None) is None
    # Unknown format is returned unchanged
    assert parse_start_time("not-a-timestamp") == "not-a-timestamp"


def test_setup_and_upsert_roundtrip(store, make_game):
    game = make_game(game_id=1, user_num=100)
    store.upsert_from_game_payload(game)

    cur = store.connection.execute("SELECT COUNT(*) FROM matches")
    assert cur.fetchone()[0] == 1

    cur = store.connection.execute("SELECT COUNT(*) FROM user_match_stats")
    assert cur.fetchone()[0] == 1

    cur = store.connection.execute("SELECT COUNT(*) FROM equipment")
    assert cur.fetchone()[0] == 2

    cur = store.connection.execute("SELECT COUNT(*) FROM mastery_levels")
    assert cur.fetchone()[0] >= 1

    cur = store.connection.execute("SELECT COUNT(*) FROM skill_levels")
    assert cur.fetchone()[0] >= 1

    cur = store.connection.execute("SELECT COUNT(*) FROM skill_orders")
    assert cur.fetchone()[0] >= 1

    # Update a value and ensure UPSERT updates existing rows
    game_updated = {**game, "gameRank": 1, "mmrGain": 20}
    store.upsert_from_game_payload(game_updated)
    row = store.connection.execute(
        "SELECT game_rank, mmr_gain FROM user_match_stats WHERE game_id=? AND user_num=?",
        (1, 100),
    ).fetchone()
    assert row[0] == 1
    assert row[1] == 20

