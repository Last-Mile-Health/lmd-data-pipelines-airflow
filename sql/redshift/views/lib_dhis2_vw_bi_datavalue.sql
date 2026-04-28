-- ═══════════════════════════════════════════════════════════
-- DHIS2 BI views for Liberia
-- ═══════════════════════════════════════════════════════════
-- Conventions:
--   • Backend table public.dhis2_data_values keeps DHIS2 UIDs (for joins/lineage).
--   • These views resolve UIDs to display names so end users only see readable
--     content. ID-hiding for API/client consumers is handled at that layer.
--   • DHIS2 stores categoryOptionCombo and attributeOptionCombo via the SAME
--     /categoryOptionCombos endpoint, so we join dhis2_dim_category_option
--     twice instead of maintaining a duplicate dim.
--   • Org unit hierarchy assumed (Liberia DHIS2):
--         Level 1 = Country, 2 = County, 3 = District, 4 = Facility
--     Adjust the SPLIT_PART indexes in lib_dhis2_orgunit_hierarchy if your
--     deployment differs.
-- ═══════════════════════════════════════════════════════════

-- ─── one-time cleanup ──────────────────────────────────────
DROP TABLE IF EXISTS public.dhis2_dim_attribute_option CASCADE;

-- Drop noisy pipeline metadata columns that leaked into the curated table.
-- Redshift has no DROP COLUMN IF EXISTS, so we use a one-shot stored
-- procedure that checks information_schema before each drop. Idempotent.
CREATE OR REPLACE PROCEDURE public._dhis2_drop_legacy_cols() AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'dhis2_data_values'
          AND column_name IN (
              '_pipeline_execution_id','_pipeline_name',
              '_country','_processed_at','_cleaned_at','_curated_execution_id','_dedup_hash'
          )
    LOOP
        EXECUTE 'ALTER TABLE public.dhis2_data_values DROP COLUMN ' || quote_ident(rec.column_name);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL public._dhis2_drop_legacy_cols();
DROP PROCEDURE public._dhis2_drop_legacy_cols();

-- ─── org unit hierarchy resolver ───────────────────────────
-- One row per org unit with every ancestor resolved to a name.
-- BI users join their fact table to this view on org_unit_id.
CREATE OR REPLACE VIEW public.lib_dhis2_orgunit_hierarchy AS
SELECT
    ou.id                                       AS org_unit_id,
    ou.displayname                              AS org_unit,
    ou.shortname                                AS org_unit_short,
    ou.level                                    AS org_unit_level,
    ou.path                                     AS org_unit_path,
    SPLIT_PART(ou.path, '/', 3)                 AS county_id,
    c.displayname                               AS county,
    SPLIT_PART(ou.path, '/', 4)                 AS district_id,
    d.displayname                               AS district,
    SPLIT_PART(ou.path, '/', 5)                 AS facility_id,
    f.displayname                               AS facility
FROM public.dhis2_dim_orgunit ou
LEFT JOIN public.dhis2_dim_orgunit c ON c.id = SPLIT_PART(ou.path, '/', 3)
LEFT JOIN public.dhis2_dim_orgunit d ON d.id = SPLIT_PART(ou.path, '/', 4)
LEFT JOIN public.dhis2_dim_orgunit f ON f.id = SPLIT_PART(ou.path, '/', 5)
;

-- ─── detailed BI view (one row per data value, names resolved) ─
CREATE OR REPLACE VIEW public.lib_cbis_datavalue AS
SELECT
    de.displayname        AS data_element,
    de.shortname          AS data_element_short,
    h.county,
    h.district,
    h.facility,
    h.org_unit,
    h.org_unit_level,
    f.period,
    f.value,
    co.displayname        AS category_option,
    ao.displayname        AS attribute_option,
    f.created             AS created_at,
    f.lastupdated         AS updated_at
FROM public.dhis2_data_values f
LEFT JOIN public.dhis2_dim_dataelement       de ON f.dataelement          = de.id
LEFT JOIN public.lib_dhis2_orgunit_hierarchy h  ON f.orgunit              = h.org_unit_id
LEFT JOIN public.dhis2_dim_category_option   co ON f.categoryoptioncombo  = co.id
LEFT JOIN public.dhis2_dim_category_option   ao ON f.attributeoptioncombo = ao.id
;

-- ─── aggregate views (drop-in for dashboards / quick filters) ──
-- Numeric coercion: DHIS2 values are text; only rows that look numeric are summed.
-- Non-numeric values (e.g. boolean strings, free text) are counted but not summed.

CREATE OR REPLACE VIEW public.lib_cbis_datavalue_by_county AS
SELECT
    h.county,
    de.displayname                                              AS data_element,
    f.period,
    COUNT(*)                                                    AS record_count,
    SUM(CASE WHEN f.value ~ '^-?[0-9]+(\.[0-9]+)?$'
             THEN f.value::DOUBLE PRECISION END)                AS total_value,
    AVG(CASE WHEN f.value ~ '^-?[0-9]+(\.[0-9]+)?$'
             THEN f.value::DOUBLE PRECISION END)                AS avg_value
FROM public.dhis2_data_values f
LEFT JOIN public.dhis2_dim_dataelement       de ON f.dataelement = de.id
LEFT JOIN public.lib_dhis2_orgunit_hierarchy h  ON f.orgunit     = h.org_unit_id
WHERE h.county IS NOT NULL
GROUP BY h.county, de.displayname, f.period
;

CREATE OR REPLACE VIEW public.lib_cbis_datavalue_by_district AS
SELECT
    h.county,
    h.district,
    de.displayname                                              AS data_element,
    f.period,
    COUNT(*)                                                    AS record_count,
    SUM(CASE WHEN f.value ~ '^-?[0-9]+(\.[0-9]+)?$'
             THEN f.value::DOUBLE PRECISION END)                AS total_value,
    AVG(CASE WHEN f.value ~ '^-?[0-9]+(\.[0-9]+)?$'
             THEN f.value::DOUBLE PRECISION END)                AS avg_value
FROM public.dhis2_data_values f
LEFT JOIN public.dhis2_dim_dataelement       de ON f.dataelement = de.id
LEFT JOIN public.lib_dhis2_orgunit_hierarchy h  ON f.orgunit     = h.org_unit_id
WHERE h.district IS NOT NULL
GROUP BY h.county, h.district, de.displayname, f.period
;
