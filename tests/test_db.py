from er_stats.db import parse_start_time


def test_parse_start_time_variants():
    assert parse_start_time("2025-10-27T23:24:03.003+0900").startswith(
        "2025-10-27T23:24:03"
    )
    assert parse_start_time("2025-10-27T23:24:03+00:00").startswith(
        "2025-10-27T23:24:03"
    )
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
        "SELECT game_rank, mmr_gain FROM user_match_stats WHERE game_id=? AND uid=?",
        (1, "100"),
    ).fetchone()
    assert row[0] == 1
    assert row[1] == 20


def test_store_mlbot(store, make_game):
    bot_user_num = 100
    game = make_game(game_id=1, user_num=bot_user_num, mlbot=True)
    store.upsert_from_game_payload(game)

    pc_user_num = 200
    game = make_game(game_id=1, user_num=pc_user_num, mlbot=False)
    store.upsert_from_game_payload(game)

    old_user_num = 300
    game = make_game(game_id=1, user_num=old_user_num, mlbot=None)
    store.upsert_from_game_payload(game)

    cur = store.connection.execute(
        "SELECT COUNT(*) FROM users WHERE uid = ? AND ml_bot = 1", (str(bot_user_num),)
    )
    assert cur.fetchone()[0] == 1

    cur = store.connection.execute(
        "SELECT COUNT(*) FROM users WHERE uid = ? AND ml_bot = 0", (str(pc_user_num),)
    )
    assert cur.fetchone()[0] == 1

    cur = store.connection.execute(
        "SELECT COUNT(*) FROM users WHERE uid = ? AND ml_bot = 0", (str(old_user_num),)
    )
    assert cur.fetchone()[0] == 1


def test_refresh_characters(store):
    payload = [
        {"characterCode": 1, "character": "Jackie", "rarity": "normal"},
        {"characterCode": 2, "character": "Aya"},
        {"characterCode": "bad", "character": 123},
    ]

    inserted = store.refresh_characters(payload)
    assert inserted == 2

    rows = store.connection.execute(
        "SELECT character_code, name FROM characters ORDER BY character_code"
    ).fetchall()
    assert [tuple(row) for row in rows] == [(1, "Jackie"), (2, "Aya")]

    store.refresh_characters(
        [
            {"characterCode": 3, "character": "Hyunwoo"},
        ]
    )
    row = store.connection.execute(
        "SELECT character_code, name FROM characters"
    ).fetchone()
    assert tuple(row) == (3, "Hyunwoo")


def test_refresh_items(store, tmp_path):
    payload = [
        {
            "code": 101101,
            "name": "Basic Sword",
            "modeType": 0,
            "itemType": "Weapon",
            "itemGrade": "Common",
            "isCompletedItem": False,
        },
        {
            "code": 201101,
            "name": "Basic Helmet",
            "modeType": 0,
            "itemType": "Armor",
            "itemGrade": "Common",
            "isCompletedItem": True,
        },
        {
            "code": "bad",
            "name": 123,
        },
    ]

    inserted = store.refresh_items(payload)
    assert inserted == 2

    rows = store.connection.execute(
        """
        SELECT item_code,
               name,
               mode_type,
               item_type,
               item_grade,
               is_completed_item
        FROM items
        ORDER BY item_code
        """
    ).fetchall()
    assert [tuple(row) for row in rows] == [
        (101101, "Basic Sword", 0, "Weapon", "Common", 0),
        (201101, "Basic Helmet", 0, "Armor", "Common", 1),
    ]

    store.refresh_items(
        [
            {
                "code": 101102,
                "name": "Upgraded Sword",
                "modeType": 1,
                "itemType": "Weapon",
                "itemGrade": "Uncommon",
                "isCompletedItem": True,
            }
        ]
    )
    row = store.connection.execute(
        "SELECT item_code, name, mode_type, item_type, item_grade, is_completed_item FROM items"
    ).fetchone()
    assert tuple(row) == (101102, "Upgraded Sword", 1, "Weapon", "Uncommon", 1)
