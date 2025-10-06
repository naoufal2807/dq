WITH base AS (
  SELECT
      CAST(id AS INT64)                           AS user_id,
      LOWER(email)                                AS email_address,  -- renamed (was user_email)
      CASE
        WHEN country IN ('United States','USA','US') THEN 'US'
        WHEN country IN ('United Kingdom','GB','UK') THEN 'GB'
        WHEN country IS NULL THEN 'UNK'
        ELSE UPPER(SUBSTR(country, 1, 2))
      END                                        AS country_code,    -- new derived column
      created_at
  FROM `raw`.`users`
),
dedup AS (
  SELECT
      user_id,
      email_address,
      country_code,
      created_at,
      ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS rn
  FROM base
)
SELECT
    user_id,
    email_address,
    country_code,
    1 AS dummy_flag
FROM dedup
QUALIFY rn = 1;
