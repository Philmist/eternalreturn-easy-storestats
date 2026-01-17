WITH deaths AS (
  SELECT
    CASE WHEN duration >= 0
      AND duration < 250 THEN
      '1-A'
    WHEN duration >= 250
      AND duration < 390 THEN
      '2-D'
    WHEN duration >= 390
      AND duration < 520 THEN
      '2-N'
    WHEN duration >= 520
      AND duration < 650 THEN
      '3-D'
    WHEN duration >= 650
      AND duration < 770 THEN
      '3-N'
    WHEN duration >= 770
      AND duration < 880 THEN
      '4-D'
    WHEN duration >= 880
      AND duration < 990 THEN
      '4-N'
    WHEN duration >= 990
      AND duration < 1090 THEN
      '5-D'
    WHEN duration >= 1090
      AND duration < 1180 THEN
      '5-N'
    WHEN duration >= 1180 THEN
      '6-'
    ELSE
      NULL
    END AS time_band
  FROM
    read_parquet ('data/parquet/participants/**/*.parquet', hive_partitioning = 1)
  WHERE
    matching_mode IN (2, 3))
SELECT
  time_band,
  COUNT(*) AS death_count,
  COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS death_ratio
FROM
  deaths
WHERE
  time_band IS NOT NULL
GROUP BY
  time_band
ORDER BY
  CASE time_band
  WHEN '1-A' THEN
    1
  WHEN '2-D' THEN
    2
  WHEN '2-N' THEN
    3
  WHEN '3-D' THEN
    4
  WHEN '3-N' THEN
    5
  WHEN '4-D' THEN
    6
  WHEN '4-N' THEN
    7
  WHEN '5-D' THEN
    8
  WHEN '5-N' THEN
    9
  WHEN '6-' THEN
    10
  ELSE
    99
  END;

