-- DHIS2 Processed → Curated business logic SQL
-- Applied by Glue Spark using the transform step convention.
-- Each step reads from __TABLE__ and produces a new __TABLE__.

-- transform_strip_metadata
-- Keep only DHIS2 source fields. Pipeline metadata (_pipeline_*, _processed_at,
-- _cleaned_at, _country, _dedup_hash) is dropped here so it never lands in the
-- curated parquet → Redshift table. processed_to_curated.py will append
-- _curated_at as the single freshness marker.
SELECT
    dataelement,
    period,
    orgunit,
    categoryoptioncombo,
    attributeoptioncombo,
    value,
    storedby,
    followup,
    created,
    lastupdated
FROM __TABLE__;
