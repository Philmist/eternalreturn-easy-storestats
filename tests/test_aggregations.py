from er_stats.aggregations import (
    character_rankings,
    equipment_rankings,
    bot_usage_statistics,
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

    chars = character_rankings(store, **ctx)
    # Should have at least characters 1 and 2
    char_nums = {row["character_num"] for row in chars}
    assert {1, 2}.issubset(char_nums)

    equips = equipment_rankings(store, min_samples=1, **ctx)
    # Items from make_game are present
    assert all("average_rank" in row and "usage_count" in row for row in equips)

    bots = bot_usage_statistics(store, min_matches=1, **ctx)
    # ml_bot defaults to 0 unless flagged
    assert all("ml_bot" in row for row in bots)

    mmr = mmr_change_statistics(store, **ctx)
    assert all("avg_mmr_gain" in row for row in mmr)

    # Flag a user as mlbot and ensure it propagates
    store.upsert_from_game_payload(
        make_game(game_id=4, user_num=13, character_num=2, game_rank=3, mlbot=True)
    )
    bots2 = bot_usage_statistics(store, min_matches=1, **ctx)
    assert any(row["ml_bot"] == 1 for row in bots2)
