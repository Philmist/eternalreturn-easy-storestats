-- ATTACH SQLite3 DB
ATTACH 'data/db.sqlite3' AS db (READONLY);

-- CREATE TEMPORARY VIEW TO QUERY SEED
CREATE TEMPORARY VIEW user_seeds AS
WITH cte AS (
  SELECT
    u.uid AS uid,
    any_value (u.nickname) AS nickname,
    any_value (u.last_mmr) AS mmr,
    count() AS games,
    COALESCE(ANY_VALUE (u.ml_bot), 0) AS ml_bot
  FROM
    db.users AS u
    JOIN db.user_match_stats AS m USING (uid)
  WHERE
    deleted = 0
    AND age(last_seen::timestamptz) <= '1 WEEK'::interval
  GROUP BY
    uid
  HAVING
    games >= 5
)
SELECT
  nickname,
  uid,
  mmr,
  games,
  ml_bot
FROM
  cte;

-- GET SEEDS
---- DEFINE MACRO
CREATE OR REPLACE TEMP MACRO seeds (
  lv,
  rv
) AS TABLE
SELECT
  nickname
FROM (
  SELECT
    nickname
  FROM
    user_seeds
  WHERE
    mmr >= lv
    AND mmr < rv)
USING SAMPLE 5;

---- EXECUTE QUERY TO GET SEEDS
SELECT
  nickname
FROM
  -- TOP TIER
  seeds (7200, 30000)
UNION
SELECT
  nickname
FROM
  -- MID TIER
  seeds (3600, 7200)
UNION
SELECT
  nickname
FROM
  -- LOW TIER
  seeds (0, 3600)
UNION
SELECT
  nickname
FROM
  user_seeds
WHERE
  ml_bot = 1
  USING SAMPLE 5;

