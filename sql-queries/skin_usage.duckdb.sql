ATTACH 'data\db.sqlite3' AS db (READONLY);

SELECT
  character_num,
  ANY_VALUE (db.characters.name) AS name,
  skin_code,
  count() AS usage_count
FROM
  read_parquet ('data/parquet/participants/**/*.parquet', union_by_name = TRUE)
    INNER JOIN db.characters ON db.characters.character_code = character_num
  GROUP BY
    character_num,
    skin_code
  ORDER BY
    usage_count DESC;

