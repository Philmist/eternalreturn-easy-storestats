import pytest

from er_stats.aggregations import (
    bot_usage_statistics,
    character_rankings,
    equipment_rankings,
    mmr_change_statistics,
)


def test_aggregations_basic(store, make_game):
    ctx = dict(season_id=25, server_name="NA", matching_mode=3, matching_team_mode=1)

    # Two users, two characters, different ranks and equipment
    store.upsert_from_game_payload(
        make_game(game_id=1, user_num=10, character_num=1, game_rank=2)
    )
    store.upsert_from_game_payload(
        make_game(game_id=2, user_num=11, character_num=1, game_rank=4)
    )
    store.upsert_from_game_payload(
        make_game(game_id=3, user_num=12, character_num=2, game_rank=1)
    )

    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
            {"characterCode": 2, "character": "Aya"},
        ]
    )

    chars = character_rankings(store, **ctx)
    # Should have at least characters 1 and 2
    char_nums = {row["character_num"] for row in chars}
    assert {1, 2}.issubset(char_nums)
    assert {row["character_name"] for row in chars if row["character_num"] == 1} == {
        "Jackie"
    }

    equips = equipment_rankings(store, min_samples=1, **ctx)
    # Items from make_game are present and enriched with item metadata when available
    assert all("average_rank" in row and "usage_count" in row for row in equips)

    bots = bot_usage_statistics(store, min_matches=1, **ctx)
    # ml_bot defaults to 0 unless flagged
    assert all("ml_bot" in row and "character_name" in row for row in bots)

    mmr = mmr_change_statistics(store, **ctx)
    assert all("avg_mmr_gain" in row and "character_name" in row for row in mmr)

    # Flag a user as mlbot and ensure it propagates
    store.upsert_from_game_payload(
        make_game(game_id=4, user_num=13, character_num=2, game_rank=3, mlbot=True)
    )
    bots2 = bot_usage_statistics(store, min_matches=1, **ctx)
    assert any(row["ml_bot"] == 1 for row in bots2)
    assert any(
        row["character_num"] == 2 and row["character_name"] == "Aya" for row in bots2
    )


def test_bot_usage_statistics_min_matches_and_context(store, make_game):
    ctx = dict(
        season_id=25,
        server_name="NA",
        matching_mode=3,
        matching_team_mode=3,
    )

    def add_player(
        game_id: int,
        user_num: int,
        character_num: int,
        game_rank: int,
        team_number: int,
        *,
        mlbot: bool | None = None,
        season_id: int = 25,
    ) -> None:
        game = make_game(
            game_id=game_id,
            user_num=user_num,
            character_num=character_num,
            game_rank=game_rank,
            matching_team_mode=3,
            season_id=season_id,
            mlbot=mlbot,
        )
        game["teamNumber"] = team_number
        store.upsert_from_game_payload(game)

    # Game 1 (context matches ctx)
    # Team 1 rank 1: BotA (Jackie) + humans
    add_player(1, 1001, 1, 1, 1, mlbot=True)
    add_player(1, 1002, 2, 1, 1)
    add_player(1, 1003, 3, 1, 1)
    # Team 2 rank 2: BotB (Fiora) + humans
    add_player(1, 1004, 4, 2, 2, mlbot=True)
    add_player(1, 1005, 5, 2, 2)
    add_player(1, 1006, 6, 2, 2)

    # Game 2 (context matches ctx)
    # Team 1 rank 2: all humans
    add_player(2, 2001, 2, 2, 1)
    add_player(2, 2002, 3, 2, 1)
    add_player(2, 2003, 6, 2, 1)
    # Team 2 rank 1: BotA and BotB again
    add_player(2, 1001, 1, 1, 2, mlbot=True)
    add_player(2, 1004, 4, 1, 2, mlbot=True)
    add_player(2, 2006, 5, 1, 2)

    # Game 3 (context matches ctx)
    # Team 1 rank 3: BotA + humans
    add_player(3, 1001, 1, 3, 1, mlbot=True)
    add_player(3, 3002, 2, 3, 1)
    add_player(3, 3003, 3, 3, 1)
    # Team 2 rank 1: BotC (LiDailin) + humans
    add_player(3, 1007, 7, 1, 2, mlbot=True)
    add_player(3, 3005, 5, 1, 2)
    add_player(3, 3006, 6, 1, 2)

    # Game 4: different season, should not count towards ctx
    add_player(4, 1001, 1, 1, 1, mlbot=True, season_id=26)

    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
            {"characterCode": 4, "character": "Fiora"},
            {"characterCode": 7, "character": "LiDailin"},
        ]
    )

    # With min_matches=2, BotA (3 matches) and BotB (2 matches) should appear,
    # BotC (1 match) should be filtered out.
    rows_min2 = bot_usage_statistics(store, min_matches=2, **ctx)
    keys_min2 = {(row["user_num"], row["character_num"]) for row in rows_min2}
    assert keys_min2 == {(1001, 1), (1004, 4)}

    # Verify BotA (Jackie): ranks [1, 1, 3] -> average 5/3, matches 3
    bot_a = next(row for row in rows_min2 if row["user_num"] == 1001)
    assert bot_a["ml_bot"] == 1
    assert bot_a["character_name"] == "Jackie"
    assert bot_a["matches"] == 3
    assert bot_a["average_rank"] == pytest.approx(5 / 3)

    # Verify BotB (Fiora): ranks [2, 1] -> average 1.5, matches 2
    bot_b = next(row for row in rows_min2 if row["user_num"] == 1004)
    assert bot_b["ml_bot"] == 1
    assert bot_b["character_name"] == "Fiora"
    assert bot_b["matches"] == 2
    assert bot_b["average_rank"] == pytest.approx(1.5)

    # With min_matches=3, only BotA remains
    rows_min3 = bot_usage_statistics(store, min_matches=3, **ctx)
    keys_min3 = {(row["user_num"], row["character_num"]) for row in rows_min3}
    assert keys_min3 == {(1001, 1)}
    only_bot = rows_min3[0]
    assert only_bot["matches"] == 3
    assert only_bot["average_rank"] == pytest.approx(5 / 3)


def test_character_rankings_three_matches_team_of_three(store, make_game):
    ctx = dict(
        season_id=25,
        server_name="NA",
        matching_mode=3,
        matching_team_mode=3,
    )

    def add_player(
        game_id: int,
        user_num: int,
        character_num: int,
        game_rank: int,
        team_number: int,
    ) -> None:
        game = make_game(
            game_id=game_id,
            user_num=user_num,
            character_num=character_num,
            game_rank=game_rank,
            matching_team_mode=3,
        )
        game["teamNumber"] = team_number
        store.upsert_from_game_payload(game)

    # Game 1: team 1 rank 1, team 2 rank 2
    add_player(1, 101, 1, 1, 1)
    add_player(1, 102, 2, 1, 1)
    add_player(1, 103, 9, 1, 1)
    add_player(1, 104, 3, 2, 2)
    add_player(1, 105, 4, 2, 2)
    add_player(1, 106, 5, 2, 2)

    # Game 2: team 1 rank 1, team 2 rank 2
    add_player(2, 201, 1, 1, 1)
    add_player(2, 202, 2, 1, 1)
    add_player(2, 203, 9, 1, 1)
    add_player(2, 204, 3, 2, 2)
    add_player(2, 205, 4, 2, 2)
    add_player(2, 206, 5, 2, 2)

    # Game 3: team 1 rank 2, team 2 rank 1
    add_player(3, 301, 1, 2, 1)
    add_player(3, 302, 2, 2, 1)
    add_player(3, 303, 9, 2, 1)
    add_player(3, 304, 3, 1, 2)
    add_player(3, 305, 4, 1, 2)
    add_player(3, 306, 5, 1, 2)

    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
            {"characterCode": 2, "character": "Aya"},
            {"characterCode": 3, "character": "Hyunwoo"},
            {"characterCode": 4, "character": "Fiora"},
            {"characterCode": 5, "character": "Zahir"},
            {"characterCode": 9, "character": "Nadine"},
        ]
    )

    rows = character_rankings(store, **ctx)
    by_char = {row["character_num"]: row for row in rows}

    # Characters 1, 2, 9: ranks [1, 1, 2] -> average 4/3
    jackie = by_char[1]
    assert jackie["character_name"] == "Jackie"
    assert jackie["matches"] == 3
    assert jackie["rank_1"] == 2
    assert jackie["rank_2_3"] == 1
    assert jackie["rank_4_6"] == 0
    assert jackie["average_rank"] == pytest.approx(4 / 3)

    # Characters 3, 4, 5: ranks [2, 2, 1] -> average 5/3
    hyunwoo = by_char[3]
    assert hyunwoo["character_name"] == "Hyunwoo"
    assert hyunwoo["matches"] == 3
    assert hyunwoo["rank_1"] == 1
    assert hyunwoo["rank_2_3"] == 2
    assert hyunwoo["rank_4_6"] == 0
    assert hyunwoo["average_rank"] == pytest.approx(5 / 3)
