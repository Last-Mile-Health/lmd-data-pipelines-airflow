-- IFI Raw → Processed cleaning SQL
-- Applied by Glue Spark after dynamic flattening/cleaning.
-- Reads from temp view "source_data" — columns are already
-- lowercased and cleaned by the Glue job.
-- All date/time outputs are TIMESTAMP for Redshift compatibility.

SELECT
    *,

    -- Clean county label
    CASE WHEN basic_info_county IS NOT NULL
         THEN LOWER(TRIM(basic_info_county))
         ELSE NULL
    END AS county,

    CASE CAST(basic_info_county AS INT)
        WHEN 1  THEN 'Bomi'
        WHEN 2  THEN 'Bong'
        WHEN 3  THEN 'Gbarpolu'
        WHEN 4  THEN 'Grand Bassa'
        WHEN 5  THEN 'Grand Cape Mount'
        WHEN 6  THEN 'Grand Gedeh'
        WHEN 7  THEN 'Grand Kru'
        WHEN 8  THEN 'Lofa'
        WHEN 9  THEN 'Margibi'
        WHEN 10 THEN 'Maryland'
        WHEN 11 THEN 'Montserrado'
        WHEN 12 THEN 'Nimba'
        WHEN 13 THEN 'Rivercess'
        WHEN 14 THEN 'River Gee'
        WHEN 15 THEN 'Sinoe'
        ELSE NULL
    END AS county_label,

    -- Managed/non-managed flag (counties 4=Grand Bassa, 13=Rivercess)
    CASE
        WHEN CAST(basic_info_county AS INT) IN (4, 13) THEN 1
        ELSE 0
    END AS managed_county,

    -- Fix known bad date entry (09feb2016 → 11jul2025)
    CASE
        WHEN TO_DATE(basic_info_date) = TO_DATE('2016-02-09')
        THEN TO_TIMESTAMP('2025-07-11')
        ELSE TO_TIMESTAMP(basic_info_date)
    END AS manual_date,

    -- Start/end as timestamps
    TO_TIMESTAMP(`start`)  AS start_date,
    TO_TIMESTAMP(`end`)    AS end_date,

    -- Manual timestamps (start uses system start, end_manual is manual entry)
    TO_TIMESTAMP(`start`)      AS s_manual_dt,
    TO_TIMESTAMP(end_manual)   AS e_manual_dt,

    -- Calendar month/year
    MONTH(TO_DATE(basic_info_date))  AS cy_month,
    YEAR(TO_DATE(basic_info_date))   AS cy_year,

    -- Fiscal year (July–June, e.g. July 2024 = FY2025)
    CASE
        WHEN MONTH(TO_DATE(basic_info_date)) >= 7
        THEN YEAR(TO_DATE(basic_info_date)) + 1
        ELSE YEAR(TO_DATE(basic_info_date))
    END AS fy_year,

    -- Fiscal month (July=1 through June=12)
    CASE
        WHEN MONTH(TO_DATE(basic_info_date)) >= 7
        THEN MONTH(TO_DATE(basic_info_date)) - 6
        ELSE MONTH(TO_DATE(basic_info_date)) + 6
    END AS fy_month,

    -- Fiscal quarter (based on calendar month)
    CASE
        WHEN MONTH(TO_DATE(basic_info_date)) IN (7,8,9)    THEN 1
        WHEN MONTH(TO_DATE(basic_info_date)) IN (10,11,12) THEN 2
        WHEN MONTH(TO_DATE(basic_info_date)) IN (1,2,3)    THEN 3
        WHEN MONTH(TO_DATE(basic_info_date)) IN (4,5,6)    THEN 4
    END AS fy_quarter,

    current_timestamp() AS _cleaned_at

FROM source_data
