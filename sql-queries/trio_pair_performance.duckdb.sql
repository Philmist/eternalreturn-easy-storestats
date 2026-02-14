-- Run from project root
ATTACH 'data/db.sqlite3' AS db (READONLY);

WITH team AS (
  SELECT
    p.game_id,
    p.team_number,
    m.matching_mode,
    array_sort (LIST (p.character_num)) AS characters_id,
    array_sort (LIST (c.name)) AS characters_name,
    list_count (LIST (p.character_num)) AS players,
    MAX(p.mmr_gain) AS mmr,
    MAX(p.victory) AS team_victory,
    MIN(p.game_rank) AS team_rank
  FROM
    db.user_match_stats AS p
    JOIN db.matches AS m ON p.game_id = m.game_id
    JOIN db.characters AS c ON c.character_code = p.character_num
  WHERE
  -- matching_mode, mode_name = [(2, normal) | (3, ranked) | (6, cobalt) | (8, union)]
  m.matching_mode IN (2, 3)
GROUP BY
  p.game_id,
  p.team_number,
  m.matching_mode
),
pairs AS (
  SELECT
    game_id,
    team_number,
    matching_mode,
    team_victory,
    team_rank,
    mmr,
    list_extract (characters_id, i + 1) AS character_id_a,
    list_extract (characters_id, j + 1) AS character_id_b,
    list_extract (characters_name, i + 1) AS character_name_a,
    list_extract (characters_name, j + 1) AS character_name_b
  FROM
    team
    CROSS JOIN RANGE (0,
      list_count (characters_id) - 1) AS i (i)
    CROSS JOIN RANGE (i + 1,
      list_count (characters_id)) AS j (j)
  WHERE
  -- matching_mode, players = [(2, 3) | (3, 3) | (6, 4) | (8, 3)]
  players = 3
)
SELECT
[character_id_a, character_id_b] AS characters_id,
[character_name_a, character_name_b] AS characters_name,
  COUNT(*) AS matches_played,
  ROUND(AVG(mmr), 2) AS mmr,
  ROUND(AVG(team_rank), 2) AS avg_rank,
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
  pairs
WHERE
  mmr >= 0
GROUP BY
  characters_id,
  characters_name
HAVING
  matches_played >= 20 -- samples
ORDER BY
  avg_rank ASC,
  win_rate DESC,
  top3_rate DESC,
  mmr DESC,
  matches_played DESC
LIMIT 20;

