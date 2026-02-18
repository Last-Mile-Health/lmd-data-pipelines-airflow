CREATE OR REPLACE VIEW public.dhis2_vw_bi_datavalue AS
SELECT
    f.dataelement,
    de.displayname   AS dataelement_name,
    de.shortname     AS dataelement_shortname,
    f.orgunit,
    ou.displayname   AS orgunit_name,
    ou.shortname     AS orgunit_shortname,
    ou.level         AS orgunit_level,
    ou.path          AS orgunit_path,
    ou.parent_id     AS orgunit_parent_id,
    ou.parent_displayname AS orgunit_parent_name,
    f.period,
    f.value,
    f.categoryoptioncombo,
    co.displayname   AS categoryoption_name,
    f.attributeoptioncombo,
    ao.displayname   AS attributeoption_name,
    f.storedby,
    f.created,
    f.lastupdated
FROM public.dhis2_data_values f
LEFT JOIN public.dhis2_dim_dataelement de
    ON f.dataelement = de.id
LEFT JOIN public.dhis2_dim_orgunit ou
    ON f.orgunit = ou.id
LEFT JOIN public.dhis2_dim_category_option co
    ON f.categoryoptioncombo = co.id
LEFT JOIN public.dhis2_dim_attribute_option ao
    ON f.attributeoptioncombo = ao.id
;
