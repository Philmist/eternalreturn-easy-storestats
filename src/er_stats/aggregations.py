"""Analytics helpers for querying the Eternal Return SQLite store."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .db import SQLiteStore


def _context_filters(
    *,
    season_id: int,
    server_name: str,
    matching_mode: int,
    matching_team_mode: int,
    start_dtm_from: Optional[str] = None,
    start_dtm_to: Optional[str] = None,
    version_major: Optional[int] = None,
) -> tuple[str, Dict[str, Any]]:
    params: Dict[str, Any] = {
        "season_id": season_id,
        "server_name": server_name,
        "matching_mode": matching_mode,
        "matching_team_mode": matching_team_mode,
    }
    clauses = [
        "m.season_id = :season_id",
        "m.server_name = :server_name",
        "m.matching_mode = :matching_mode",
        "m.matching_team_mode = :matching_team_mode",
    ]
    if start_dtm_from is not None:
        params["start_dtm_from"] = start_dtm_from
        clauses.append("unixepoch(m.start_dtm, 'auto') >= unixepoch(:start_dtm_from)")
    if start_dtm_to is not None:
        params["start_dtm_to"] = start_dtm_to
        clauses.append("unixepoch(m.start_dtm, 'auto') < unixepoch(:start_dtm_to)")
    if version_major is not None:
        params["version_major"] = version_major
        clauses.append("m.version_major = :version_major")
    where_clause = " WHERE " + " AND ".join(clauses)
    return where_clause, params


def character_rankings(
    store: SQLiteStore,
    *,
    season_id: int,
    server_name: str,
    matching_mode: int,
    matching_team_mode: int,
    start_dtm_from: Optional[str] = None,
    start_dtm_to: Optional[str] = None,
    version_major: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Return average rank and distribution per character."""

    where_clause, params = _context_filters(
        season_id=season_id,
        server_name=server_name,
        matching_mode=matching_mode,
        matching_team_mode=matching_team_mode,
        start_dtm_from=start_dtm_from,
        start_dtm_to=start_dtm_to,
        version_major=version_major,
    )
    query = f"""
        WITH filtered AS (
            SELECT ums.game_id, ums.user_num, ums.character_num, ums.game_rank
            FROM user_match_stats AS ums
            JOIN matches AS m ON m.game_id = ums.game_id
            {where_clause}
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
        params,
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
    start_dtm_from: Optional[str] = None,
    start_dtm_to: Optional[str] = None,
    version_major: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Compute average rank per equipment item."""

    where_clause, params = _context_filters(
        season_id=season_id,
        server_name=server_name,
        matching_mode=matching_mode,
        matching_team_mode=matching_team_mode,
        start_dtm_from=start_dtm_from,
        start_dtm_to=start_dtm_to,
        version_major=version_major,
    )
    params["min_samples"] = min_samples
    query = f"""
        WITH filtered AS (
            SELECT ums.game_id, ums.user_num, ums.game_rank
            FROM user_match_stats AS ums
            JOIN matches AS m ON m.game_id = ums.game_id
            {where_clause}
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
        params,
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
    start_dtm_from: Optional[str] = None,
    start_dtm_to: Optional[str] = None,
    version_major: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Return bot usage and average rank per character across all bot users."""

    where_clause, params = _context_filters(
        season_id=season_id,
        server_name=server_name,
        matching_mode=matching_mode,
        matching_team_mode=matching_team_mode,
        start_dtm_from=start_dtm_from,
        start_dtm_to=start_dtm_to,
        version_major=version_major,
    )
    params["min_matches"] = min_matches
    query = f"""
        WITH filtered AS (
            SELECT ums.*, m.season_id
            FROM user_match_stats AS ums
            JOIN matches AS m ON m.game_id = ums.game_id
            {where_clause}
        )
        SELECT MAX(COALESCE(f.ml_bot, 0)) AS ml_bot,
               f.character_num,
               c.name AS character_name,
               AVG(f.game_rank) AS average_rank,
               COUNT(*) AS matches
        FROM filtered AS f
        LEFT JOIN characters AS c ON c.character_code = f.character_num
        WHERE f.ml_bot = 1
        GROUP BY f.character_num, c.name
        HAVING matches >= :min_matches
        ORDER BY matches DESC
    """
    cur = store.connection.execute(
        query,
        params,
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
    start_dtm_from: Optional[str] = None,
    start_dtm_to: Optional[str] = None,
    version_major: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Additional aggregation showing mean MMR gain per character."""

    where_clause, params = _context_filters(
        season_id=season_id,
        server_name=server_name,
        matching_mode=matching_mode,
        matching_team_mode=matching_team_mode,
        start_dtm_from=start_dtm_from,
        start_dtm_to=start_dtm_to,
        version_major=version_major,
    )
    query = f"""
        WITH filtered AS (
            SELECT ums.character_num,
                   ums.mmr_gain,
                   ums.mmr_loss_entry_cost
            FROM user_match_stats AS ums
            JOIN matches AS m ON m.game_id = ums.game_id
            {where_clause}
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
        params,
    )
    rows = cur.fetchall()
    return [dict(row) for row in rows]


__all__ = [
    "character_rankings",
    "equipment_rankings",
    "bot_usage_statistics",
    "mmr_change_statistics",
]
