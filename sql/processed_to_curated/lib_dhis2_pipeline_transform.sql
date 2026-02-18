-- DHIS2 Processed → Curated business logic SQL
-- Applied by Glue Spark using the transform step convention.
-- Each step reads from __TABLE__ and produces a new __TABLE__.
--
-- Customize this file with DHIS2-specific business logic.

-- transform_passthrough
-- Pass through all columns (customize as needed)
SELECT
    *
FROM __TABLE__;
