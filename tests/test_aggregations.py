import pytest

from er_stats.aggregations import (
    bot_usage_statistics,
    character_rankings,
    equipment_rankings,
    mmr_change_statistics,
    team_composition_statistics,
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
        season_id=0,
        server_name="Asia3",
        matching_mode=2,
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
        season_id: int = 0,
    ) -> None:
        game = make_game(
            game_id=game_id,
            user_num=user_num,
            character_num=character_num,
            game_rank=game_rank,
            matching_mode=2,
            matching_team_mode=3,
            server_name="Asia3",
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
    keys_min2 = {row["character_num"] for row in rows_min2}
    assert keys_min2 == {1, 4}

    # Verify BotA (Jackie): ranks [1, 1, 3] -> average 5/3, matches 3
    bot_a = next(row for row in rows_min2 if row["character_num"] == 1)
    assert bot_a["ml_bot"] == 1
    assert bot_a["character_name"] == "Jackie"
    assert bot_a["matches"] == 3
    assert bot_a["average_rank"] == pytest.approx(5 / 3)

    # Verify BotB (Fiora): ranks [2, 1] -> average 1.5, matches 2
    bot_b = next(row for row in rows_min2 if row["character_num"] == 4)
    assert bot_b["ml_bot"] == 1
    assert bot_b["character_name"] == "Fiora"
    assert bot_b["matches"] == 2
    assert bot_b["average_rank"] == pytest.approx(1.5)

    # With min_matches=3, only BotA remains
    rows_min3 = bot_usage_statistics(store, min_matches=3, **ctx)
    keys_min3 = {row["character_num"] for row in rows_min3}
    assert keys_min3 == {1}
    only_bot = rows_min3[0]
    assert only_bot["matches"] == 3
    assert only_bot["average_rank"] == pytest.approx(5 / 3)


def test_character_rankings_filters_by_time_window(store, make_game):
    ctx = dict(season_id=25, server_name="NA", matching_mode=3, matching_team_mode=1)

    early_game = make_game(
        game_id=101, user_num=1, character_num=1, game_rank=1, season_id=25
    )
    early_game["startDtm"] = "2025-11-24T23:00:00+09:00"  # 14:00Z
    store.upsert_from_game_payload(early_game)

    later_game = make_game(
        game_id=102, user_num=2, character_num=2, game_rank=2, season_id=25
    )
    later_game["startDtm"] = "2025-11-24T15:00:00+00:00"  # 15:00Z
    store.upsert_from_game_payload(later_game)

    all_rows = character_rankings(store, **ctx)
    assert {row["character_num"] for row in all_rows} == {1, 2}

    filtered_rows = character_rankings(
        store,
        start_dtm_from="2025-11-24T14:30:00+00:00",
        **ctx,
    )
    assert {row["character_num"] for row in filtered_rows} == {2}


def test_character_rankings_filters_by_version_major(store, make_game):
    ctx = dict(season_id=25, server_name="NA", matching_mode=3, matching_team_mode=1)

    game_v1 = make_game(
        game_id=201, user_num=3, character_num=3, game_rank=1, season_id=25
    )
    game_v1["versionMajor"] = 1
    store.upsert_from_game_payload(game_v1)

    game_v2 = make_game(
        game_id=202, user_num=4, character_num=4, game_rank=2, season_id=25
    )
    game_v2["versionMajor"] = 2
    store.upsert_from_game_payload(game_v2)

    all_rows = character_rankings(store, **ctx)
    assert {row["character_num"] for row in all_rows} == {3, 4}

    v2_rows = character_rankings(store, version_major=2, **ctx)
    assert {row["character_num"] for row in v2_rows} == {4}


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


def test_team_composition_statistics_includes_all_servers(store, make_game):
    def add_player(
        game_id: int,
        team_number: int,
        user_num: int,
        character_num: int,
        game_rank: int,
        server_name: str,
    ) -> None:
        game = make_game(
            game_id=game_id,
            user_num=user_num,
            character_num=character_num,
            game_rank=game_rank,
            matching_team_mode=3,
            server_name=server_name,
        )
        game["teamNumber"] = team_number
        store.upsert_from_game_payload(game)

    # Game 1 on NA: team A wins, team B loses.
    add_player(1, 1, 101, 1, 1, "NA")
    add_player(1, 1, 102, 2, 1, "NA")
    add_player(1, 1, 103, 3, 1, "NA")
    add_player(1, 2, 104, 4, 2, "NA")
    add_player(1, 2, 105, 5, 2, "NA")
    add_player(1, 2, 106, 6, 2, "NA")

    # Game 2 on Asia: team A loses, team C wins.
    add_player(2, 1, 201, 1, 2, "Asia")
    add_player(2, 1, 202, 2, 2, "Asia")
    add_player(2, 1, 203, 3, 2, "Asia")
    add_player(2, 2, 204, 7, 1, "Asia")
    add_player(2, 2, 205, 8, 1, "Asia")
    add_player(2, 2, 206, 9, 1, "Asia")

    store.refresh_characters(
        [
            {"characterCode": 1, "character": "Jackie"},
            {"characterCode": 2, "character": "Aya"},
            {"characterCode": 3, "character": "Hyunwoo"},
            {"characterCode": 4, "character": "Fiora"},
            {"characterCode": 5, "character": "Zahir"},
            {"characterCode": 6, "character": "Hyejin"},
            {"characterCode": 7, "character": "LiDailin"},
            {"characterCode": 8, "character": "Isol"},
            {"characterCode": 9, "character": "Xiukai"},
        ]
    )

    rows = team_composition_statistics(
        store,
        season_id=25,
        server_name=None,
        matching_mode=3,
        matching_team_mode=3,
        top_n=3,
        min_matches=2,
    )
    assert len(rows) == 1
    comp = rows[0]
    assert comp["team_signature"] == "1+2+3"
    assert comp["character_nums"] == [1, 2, 3]
    assert comp["character_names"] == ["Jackie", "Aya", "Hyunwoo"]
    assert comp["matches"] == 2
    assert comp["wins"] == 1
    assert comp["top_finishes"] == 2
    assert comp["win_rate"] == pytest.approx(0.5)
    assert comp["top_rate"] == pytest.approx(1.0)
    assert comp["average_rank"] == pytest.approx(1.5)

    rows_no_names = team_composition_statistics(
        store,
        season_id=25,
        server_name=None,
        matching_mode=3,
        matching_team_mode=3,
        top_n=3,
        min_matches=1,
        include_names=False,
        sort_by="avg-rank",
    )
    assert any(row["team_signature"] == "1+2+3" for row in rows_no_names)
    for row in rows_no_names:
        assert "character_names" not in row
