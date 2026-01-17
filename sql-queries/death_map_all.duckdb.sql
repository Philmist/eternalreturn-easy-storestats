WITH deaths AS (
  SELECT
    NULLIF (place_of_death, '') AS place_of_death,
    NULLIF (area.name, '') AS place_name
  FROM
    read_parquet ('data/parquet/participants/**/*.parquet', union_by_name = TRUE)
    LEFT JOIN read_csv ('constants/area_code.csv') AS area ON area.code = place_of_death
  WHERE
    matching_mode IN (2, 3))
SELECT
  place_of_death,
  COALESCE(ANY_VALUE(place_name), '') AS place_name,
  COUNT(*) AS death_count,
  COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS death_ratio
FROM
  deaths
WHERE
  place_of_death IS NOT NULL
GROUP BY
  place_of_death
ORDER BY
  death_count DESC;

