-- Run from project root.
-- [ingest].db_path = "data/db.sqlite3"
ATTACH 'data/db.sqlite3' AS db (READONLY);

WITH params AS (
  SELECT
    -- Required: fixed character.
    'Eleven'::text AS anchor_character_name,
    -- Optional: set to NULL to list all partners for the anchor.
    -- Example: 'Jackie'::text
    NULL::text AS partner_character_name,
    -- Minimum sample size for output rows.
    20::integer AS min_samples
),
team AS (
  SELECT
    p.game_id,
    p.team_number,
    m.matching_mode,
    array_sort (LIST (p.character_num)) AS characters_id,
    array_sort (LIST (c.name)) AS characters_name,
    list_count (LIST (p.character_num)) AS players,
    MAX(p.mmr_gain) AS team_mmr_gain,
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
anchor_teams AS (
  SELECT
    t.game_id,
    t.team_number,
    t.characters_id,
    t.characters_name,
    t.team_mmr_gain,
    t.team_victory,
    t.team_rank
  FROM
    team AS t
    CROSS JOIN params AS p
  WHERE
  -- matching_mode, players = [(2, 3) | (3, 3) | (6, 4) | (8, 3)]
  t.players = 3
  AND t.team_mmr_gain >= 0
  AND list_contains (t.characters_name, p.anchor_character_name)
),
partners AS (
  SELECT
    list_extract (a.characters_name, i + 1) AS partner_character_name,
    a.team_mmr_gain,
    a.team_victory,
    a.team_rank
  FROM
    anchor_teams AS a
    CROSS JOIN RANGE (0,
      list_count (a.characters_name)) AS idx (i)
    CROSS JOIN params AS p
  WHERE
    list_extract (a.characters_name, i + 1) <> p.anchor_character_name
    AND (p.partner_character_name IS NULL
      OR list_extract (a.characters_name, i + 1) = p.partner_character_name))
SELECT
  p.anchor_character_name,
  pr.partner_character_name,
  COUNT(*) AS matches_played,
  ROUND(SUM(
      CASE WHEN pr.team_victory = 1 THEN
        1
      ELSE
        0
      END)::double / COUNT(*), 4) AS win_rate,
  ROUND(SUM(
      CASE WHEN pr.team_rank <= 3 THEN
        1
      ELSE
        0
      END)::double / COUNT(*), 4) AS top3_rate,
  ROUND(AVG(pr.team_mmr_gain), 2) AS avg_mmr_gain,
  ROUND(AVG(pr.team_rank), 2) AS avg_team_rank
FROM
  partners AS pr
  CROSS JOIN params AS p
GROUP BY
  p.anchor_character_name,
  pr.partner_character_name
HAVING
  COUNT(*) >= MAX(p.min_samples)
ORDER BY
  win_rate DESC,
  top3_rate DESC,
  avg_mmr_gain DESC,
  matches_played DESC
LIMIT 30;

