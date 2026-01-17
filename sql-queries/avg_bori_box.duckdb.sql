WITH team_boxes AS (
  -- チームごとにBORI箱数を合計（マップの値を合算）
  SELECT
  game_id,
  team_number,
  reduce(map_values(get_bori_reward), lambda s, v: s + COALESCE(v, 0), 0) AS boxes_from_bori
  FROM 'data/parquet/participants/**/*.parquet'
),
match_boxes AS (
  -- 試合内の全チーム合計
  SELECT game_id, SUM(boxes_from_bori) AS boxes_per_match
  FROM team_boxes
  GROUP BY game_id
)
-- 1試合あたりの平均箱数
SELECT AVG(boxes_per_match) AS avg_bori_boxes_per_match
FROM match_boxes;
