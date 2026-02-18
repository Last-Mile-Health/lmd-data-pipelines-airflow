-- DHIS2 Raw → Processed cleaning SQL
-- Applied by Glue Spark after dynamic flattening/cleaning.
-- Reads from temp view "source_data" — columns are already
-- lowercased and cleaned by the Glue job.
--
-- Customize this file with DHIS2-specific cleaning logic.

SELECT
    *,
    CURRENT_TIMESTAMP() AS _cleaned_at
FROM source_data
