"""Analytics helpers for querying the Eternal Return SQLite store."""

from __future__ import annotations

from typing import Any, Dict, List

from .db import SQLiteStore


def _context_filter_clause() -> str:
    return (
        "WHERE m.season_id = :season_id "
        "AND m.server_name = :server_name "
        "AND m.matching_mode = :matching_mode "
        "AND m.matching_team_mode = :matching_team_mode"
    )


def character_rankings(
    store: SQLiteStore,
    *,
    season_id: int,
    server_name: str,
    matching_mode: int,
    matching_team_mode: int,
) -> List[Dict[str, Any]]:
    """Return average rank and distribution per character."""

    query = f"""
        WITH filtered AS (
            SELECT ums.game_id, ums.user_num, ums.character_num, ums.game_rank
            FROM user_match_stats AS ums
            JOIN matches AS m ON m.game_id = ums.game_id
            {_context_filter_clause()}
        )
        SELECT f.character_num,
               c.name AS character_name,
               AVG(f.game_rank) AS average_rank,
               SUM(CASE WHEN f.game_rank = 1 THEN 1 ELSE 0 END) AS rank_1,
               SUM(CASE WHEN f.game_rank BETWEEN 2 AND 3 THEN 1 ELSE 0 END) AS rank_2_3,
               SUM(CASE WHEN f.game_rank BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS rank_4_6,
               COUNT(*) AS matches
        FROM filtered AS f
        LEFT JOIN characters AS c ON c.character_code = f.character_num
        GROUP BY f.character_num, c.name
        HAVING matches > 0
        ORDER BY average_rank ASC
    """
    cur = store.connection.execute(
        query,
        {
            "season_id": season_id,
            "server_name": server_name,
            "matching_mode": matching_mode,
            "matching_team_mode": matching_team_mode,
        },
    )
    rows = cur.fetchall()
    return [dict(row) for row in rows]


def equipment_rankings(
    store: SQLiteStore,
    *,
    season_id: int,
    server_name: str,
    matching_mode: int,
    matching_team_mode: int,
    min_samples: int = 5,
) -> List[Dict[str, Any]]:
    """Compute average rank per equipment item."""

    query = f"""
        WITH filtered AS (
            SELECT ums.game_id, ums.user_num, ums.game_rank
            FROM user_match_stats AS ums
            JOIN matches AS m ON m.game_id = ums.game_id
            {_context_filter_clause()}
        )
        SELECT e.item_id,
               i.name AS item_name,
               i.item_type,
               i.item_grade,
               i.is_completed_item,
               AVG(f.game_rank) AS average_rank,
               COUNT(*) AS usage_count,
               AVG(e.grade) AS average_grade
        FROM filtered AS f
        JOIN equipment AS e
          ON e.game_id = f.game_id AND e.user_num = f.user_num
        LEFT JOIN items AS i
          ON i.item_code = e.item_id
        GROUP BY e.item_id
        HAVING usage_count >= :min_samples
        ORDER BY average_rank ASC
    """
    cur = store.connection.execute(
        query,
        {
            "season_id": season_id,
            "server_name": server_name,
            "matching_mode": matching_mode,
            "matching_team_mode": matching_team_mode,
            "min_samples": min_samples,
        },
    )
    rows = cur.fetchall()
    return [dict(row) for row in rows]


def bot_usage_statistics(
    store: SQLiteStore,
    *,
    season_id: int,
    server_name: str,
    matching_mode: int,
    matching_team_mode: int,
    min_matches: int = 3,
) -> List[Dict[str, Any]]:
    """Return bot usage and average rank per character."""

    query = f"""
        WITH filtered AS (
            SELECT ums.*, m.season_id
            FROM user_match_stats AS ums
            JOIN matches AS m ON m.game_id = ums.game_id
            {_context_filter_clause()}
        )
        SELECT f.user_num,
               MAX(COALESCE(f.ml_bot, 0)) AS ml_bot,
               f.character_num,
               c.name AS character_name,
               AVG(f.game_rank) AS average_rank,
               COUNT(*) AS matches
        FROM filtered AS f
        LEFT JOIN characters AS c ON c.character_code = f.character_num
        WHERE f.ml_bot = 1
        GROUP BY f.user_num, f.character_num, c.name
        HAVING matches >= :min_matches
        ORDER BY ml_bot DESC, matches DESC
    """
    cur = store.connection.execute(
        query,
        {
            "season_id": season_id,
            "server_name": server_name,
            "matching_mode": matching_mode,
            "matching_team_mode": matching_team_mode,
            "min_matches": min_matches,
        },
    )
    rows = cur.fetchall()
    return [dict(row) for row in rows]


def mmr_change_statistics(
    store: SQLiteStore,
    *,
    season_id: int,
    server_name: str,
    matching_mode: int,
    matching_team_mode: int,
) -> List[Dict[str, Any]]:
    """Additional aggregation showing mean MMR gain per character."""

    query = f"""
        WITH filtered AS (
            SELECT ums.character_num,
                   ums.mmr_gain,
                   ums.mmr_loss_entry_cost
            FROM user_match_stats AS ums
            JOIN matches AS m ON m.game_id = ums.game_id
            {_context_filter_clause()}
        )
        SELECT f.character_num,
               c.name AS character_name,
               AVG(f.mmr_gain) AS avg_mmr_gain,
               AVG(f.mmr_loss_entry_cost) AS avg_entry_cost,
               COUNT(*) AS matches
        FROM filtered AS f
        LEFT JOIN characters AS c ON c.character_code = f.character_num
        GROUP BY f.character_num, c.name
        HAVING matches > 0
        ORDER BY avg_mmr_gain DESC
    """
    cur = store.connection.execute(
        query,
        {
            "season_id": season_id,
            "server_name": server_name,
            "matching_mode": matching_mode,
            "matching_team_mode": matching_team_mode,
        },
    )
    rows = cur.fetchall()
    return [dict(row) for row in rows]


__all__ = [
    "character_rankings",
    "equipment_rankings",
    "bot_usage_statistics",
    "mmr_change_statistics",
]
