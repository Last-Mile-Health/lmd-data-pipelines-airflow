-- ═══════════════════════════════════════════════════════════
-- DHIS2 Dimension Tables — Reference DDL
-- These tables are auto-created at runtime by redshift_load.py
-- from the column definitions in lib_dhis2_pipeline.yml.
-- This file is for documentation purposes only.
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS public.dhis2_dim_dataelement (
    "id"                        VARCHAR(64),
    "displayname"               VARCHAR(512),
    "shortname"                 VARCHAR(512),
    "categorycombo_id"          VARCHAR(64),
    "categorycombo_displayname" VARCHAR(512)
);

CREATE TABLE IF NOT EXISTS public.dhis2_dim_orgunit (
    "id"                  VARCHAR(64),
    "displayname"         VARCHAR(512),
    "shortname"           VARCHAR(512),
    "level"               INTEGER,
    "path"                VARCHAR(2048),
    "parent_id"           VARCHAR(64),
    "parent_displayname"  VARCHAR(512)
);

-- DHIS2 stores both categoryOptionCombo and attributeOptionCombo via the
-- same /categoryOptionCombos endpoint, so we maintain a single dim and join
-- it twice in the BI view (see lib_dhis2_vw_bi_datavalue.sql).
CREATE TABLE IF NOT EXISTS public.dhis2_dim_category_option (
    "id"          VARCHAR(64),
    "displayname" VARCHAR(512)
);
