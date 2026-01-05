"""Analytics helpers for querying the Eternal Return SQLite store."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .db import SQLiteStore

RANKED_MATCHING_MODE = 3
RANKED_TEAM_MODE = 3
MMR_TIERS: List[Tuple[str, int, Optional[int]]] = [
    ("Iron", 0, 600),
    ("Bronze", 600, 1400),
    ("Silver", 1400, 2400),
    ("Gold", 2400, 3600),
    ("Platinum", 3600, 5000),
    ("Diamond", 5000, 6400),
    ("Meteorite", 6400, 7200),
    ("Mythril", 7200, None),
]


def _context_filters(
    *,
    season_id: int,
    server_name: Optional[str],
    matching_mode: int,
    matching_team_mode: int,
    start_dtm_from: Optional[str] = None,
    start_dtm_to: Optional[str] = None,
    version_major: Optional[int] = None,
) -> tuple[str, Dict[str, Any]]:
    params: Dict[str, Any] = {
        "season_id": season_id,
        "matching_mode": matching_mode,
        "matching_team_mode": matching_team_mode,
    }
    clauses = [
        "m.season_id = :season_id",
        "m.matching_mode = :matching_mode",
        "m.matching_team_mode = :matching_team_mode",
        "m.incomplete = 0",
    ]
    if server_name is not None:
        params["server_name"] = server_name
        clauses.append("m.server_name = :server_name")
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


def resolve_latest_ranked_season_id(
    store: SQLiteStore,
    *,
    matching_mode: int = RANKED_MATCHING_MODE,
    matching_team_mode: int = RANKED_TEAM_MODE,
) -> int:
    """Return the latest season ID with ranked matches."""

    query = """
        SELECT MAX(season_id) AS max_season
        FROM matches
        WHERE matching_mode = :matching_mode
          AND matching_team_mode = :matching_team_mode
          AND season_id IS NOT NULL
    """
    cur = store.connection.execute(
        query,
        {
            "matching_mode": matching_mode,
            "matching_team_mode": matching_team_mode,
        },
    )
    row = cur.fetchone()
    max_season = row["max_season"] if row is not None else None
    if max_season is None:
        raise ValueError("No ranked matches found to resolve latest season.")
    return int(max_season)


def _tier_index_for_mmr(mmr: int) -> Optional[int]:
    for index, (_, min_mmr, max_mmr) in enumerate(MMR_TIERS):
        if mmr < min_mmr:
            continue
        if max_mmr is None or mmr < max_mmr:
            return index
    return None


def mmr_tier_distribution(store: SQLiteStore) -> Dict[str, Any]:
    """Return ranked MMR tier distribution for the latest season."""

    season_id = resolve_latest_ranked_season_id(store)
    query = """
        WITH eligible_users AS (
            SELECT DISTINCT ums.uid
            FROM user_match_stats AS ums
            JOIN matches AS m ON m.game_id = ums.game_id
            WHERE m.season_id = :season_id
              AND m.matching_mode = :matching_mode
              AND m.matching_team_mode = :matching_team_mode
              AND m.incomplete = 0
              AND COALESCE(ums.ml_bot, 0) = 0
        )
        SELECT u.uid,
               u.last_mmr
        FROM users AS u
        JOIN eligible_users AS e ON e.uid = u.uid
        WHERE u.deleted = 0
          AND COALESCE(u.ml_bot, 0) = 0
          AND u.last_mmr IS NOT NULL
          AND u.last_mmr >= 0
    """
    cur = store.connection.execute(
        query,
        {
            "season_id": season_id,
            "matching_mode": RANKED_MATCHING_MODE,
            "matching_team_mode": RANKED_TEAM_MODE,
        },
    )
    rows = cur.fetchall()
    total_users = len(rows)
    counts = [0 for _ in MMR_TIERS]
    for row in rows:
        mmr_value = row["last_mmr"]
        if mmr_value is None:
            continue
        tier_index = _tier_index_for_mmr(int(mmr_value))
        if tier_index is None:
            continue
        counts[tier_index] += 1

    tiers = []
    for (name, min_mmr, max_mmr), count in zip(MMR_TIERS, counts):
        ratio = count / total_users if total_users else 0.0
        tiers.append(
            {
                "tier": name,
                "min_mmr": min_mmr,
                "max_mmr": max_mmr,
                "count": count,
                "ratio": ratio,
            }
        )

    return {
        "season_id": season_id,
        "matching_mode": RANKED_MATCHING_MODE,
        "matching_team_mode": RANKED_TEAM_MODE,
        "total_users": total_users,
        "tiers": tiers,
    }


def character_rankings(
    store: SQLiteStore,
    *,
    season_id: int,
    server_name: Optional[str],
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
            SELECT ums.game_id, ums.uid, ums.character_num, ums.game_rank
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
    server_name: Optional[str],
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
            SELECT ums.game_id, ums.uid, ums.game_rank
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
          ON e.game_id = f.game_id AND e.uid = f.uid
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
    server_name: Optional[str],
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
    server_name: Optional[str],
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


def team_composition_statistics(
    store: SQLiteStore,
    *,
    season_id: int,
    matching_mode: int,
    matching_team_mode: int,
    top_n: int = 3,
    min_matches: int = 5,
    server_name: Optional[str] = None,
    start_dtm_from: Optional[str] = None,
    start_dtm_to: Optional[str] = None,
    version_major: Optional[int] = None,
    include_names: bool = True,
    sort_by: str = "win-rate",
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Compute win/top rates for team compositions."""

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
        SELECT ums.game_id,
               ums.team_number,
               ums.character_num,
               ums.game_rank,
               ums.victory
        FROM user_match_stats AS ums
        JOIN matches AS m ON m.game_id = ums.game_id
        {where_clause}
    """
    cur = store.connection.execute(query, params)
    rows = cur.fetchall()

    teams: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for row in rows:
        key = (int(row["game_id"]), int(row["team_number"]))
        team = teams.setdefault(
            key,
            {"ranks": [], "characters": [], "victory": 0},
        )
        rank = row["game_rank"]
        if rank is not None:
            team["ranks"].append(int(rank))
        victory_raw = row["victory"]
        if victory_raw is not None:
            team["victory"] = max(team["victory"], int(bool(victory_raw)))
        character = row["character_num"]
        if character is not None:
            team["characters"].append(int(character))

    char_map: Dict[int, str] = {}
    with store.cursor() as name_cur:
        name_cur.execute("SELECT character_code, name FROM characters")
        for name_row in name_cur.fetchall():
            char_map[int(name_row["character_code"])] = name_row["name"]

    compositions: Dict[Tuple[int, ...], Dict[str, Any]] = {}
    for team in teams.values():
        if not team["characters"] or not team["ranks"]:
            continue
        signature_tuple = tuple(sorted(team["characters"]))
        signature = "+".join(str(c) for c in signature_tuple)
        team_rank = min(team["ranks"])

        agg = compositions.setdefault(
            signature_tuple,
            {
                "team_signature": signature,
                "character_nums": list(signature_tuple),
                "matches": 0,
                "wins": 0,
                "top_finishes": 0,
                "sum_ranks": 0.0,
                "members": len(signature_tuple),
            },
        )
        agg["matches"] += 1
        agg["wins"] += team["victory"]
        if team_rank <= top_n:
            agg["top_finishes"] += 1
        agg["sum_ranks"] += team_rank

    results: List[Dict[str, Any]] = []
    for comp in compositions.values():
        matches = comp["matches"]
        if matches < min_matches:
            continue
        average_rank = comp["sum_ranks"] / matches if matches else None
        win_rate = comp["wins"] / matches if matches else 0.0
        top_rate = comp["top_finishes"] / matches if matches else 0.0

        row: Dict[str, Any] = {
            "team_signature": comp["team_signature"],
            "character_nums": comp["character_nums"],
            "members": comp["members"],
            "matches": matches,
            "wins": comp["wins"],
            "top_n": top_n,
            "top_finishes": comp["top_finishes"],
            "win_rate": win_rate,
            "top_rate": top_rate,
            "average_rank": average_rank,
        }
        if include_names:
            row["character_names"] = [
                char_map.get(num) for num in comp["character_nums"]
            ]
        results.append(row)

    def sort_key(value: Dict[str, Any]) -> tuple:
        if sort_by == "top-rate":
            return (-value["top_rate"], -value["win_rate"], -value["matches"])
        if sort_by == "avg-rank":
            return (
                value["average_rank"]
                if value["average_rank"] is not None
                else float("inf"),
                -value["matches"],
            )
        return (-value["win_rate"], -value["top_rate"], -value["matches"])

    results.sort(key=sort_key)
    if limit is not None and limit >= 0:
        results = results[:limit]
    return results


__all__ = [
    "character_rankings",
    "equipment_rankings",
    "bot_usage_statistics",
    "mmr_change_statistics",
    "mmr_tier_distribution",
    "team_composition_statistics",
]
