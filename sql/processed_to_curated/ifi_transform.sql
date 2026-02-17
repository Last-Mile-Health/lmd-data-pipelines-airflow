-- IFI Processed → Curated business logic SQL
-- Applied by Glue Spark using the transform step convention.
-- Each step reads from __TABLE__ and produces a new __TABLE__.

-- transform_datetime_columns
-- Convert ISO 8601 timestamps to Spark TIMESTAMP type
SELECT *,
       TO_TIMESTAMP(`start`) AS start_timestamp,
       TO_TIMESTAMP(`end`) AS end_timestamp
FROM __TABLE__;

-- transform_date_columns
-- Convert string dates to DATE type
SELECT *,
       TO_DATE(today) AS today_date
FROM __TABLE__;

-- transform_business_logic
-- Apply IFI-specific business rules
SELECT
    *
FROM __TABLE__;
