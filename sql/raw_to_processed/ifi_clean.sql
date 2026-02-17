-- IFI Raw → Processed cleaning SQL
-- Applied by Glue Spark after dynamic flattening/cleaning.
-- Reads from temp view "source_data" — columns are already
-- lowercased and cleaned by the Glue job.
--
-- Use SELECT * to keep all columns, or explicitly select/rename
-- columns for pipeline-specific transformations.

SELECT
    *,
    CASE WHEN basic_info_county IS NOT NULL
         THEN LOWER(TRIM(basic_info_county))
         ELSE NULL
    END AS county,
    CURRENT_TIMESTAMP() AS _cleaned_at
FROM source_data
