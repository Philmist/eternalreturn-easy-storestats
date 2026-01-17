-- Run from project root
ATTACH 'data/db.sqlite3' AS db (READONLY);

WITH team AS (
  SELECT
    p.game_id,
    p.team_number,
    m.matching_mode,
    array_sort (LIST (p.character_num)) AS characters_id,
    array_sort (LIST (c.name)) AS characters_name,
    COUNT(*) AS members,
    MAX(p.mmr_gain) AS mmr,
    MAX(p.victory) AS team_victory,
    MIN(p.game_rank) AS team_rank
  FROM
    db.user_match_stats AS p
    JOIN db.matches AS m ON p.game_id = m.game_id
    JOIN db.characters AS c ON c.character_code = p.character_num
    -- matching_mode, mode_name = [(2, normal) | (3, ranked) | (6, cobalt) | (8, union)]
  WHERE
    m.matching_mode IN (2, 3)
  GROUP BY
    p.game_id,
    p.team_number,
    m.matching_mode
    -- ORDER BY team_rank ASC
)
SELECT
  characters_id,
  characters_name,
  COUNT(*) AS matches_played,
  ROUND(AVG(mmr), 2) AS mmr,
  ROUND(SUM(
      CASE WHEN team_victory = 1 THEN
        1
      ELSE
        0
      END)::double / COUNT(*), 4) AS win_rate,
  ROUND(SUM(
      CASE WHEN team_rank <= 3 THEN
        1
      ELSE
        0
      END)::double / COUNT(*), 4) AS top3_rate
FROM
  team
  -- WHERE characters_id && [58]
  -- WHERE mmr >= 5000
GROUP BY
  characters_id,
  characters_name
HAVING
  matches_played >= 10 -- samples
ORDER BY
  win_rate DESC,
  matches_played DESC
LIMIT 20;

