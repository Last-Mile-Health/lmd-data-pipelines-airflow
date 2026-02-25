-- ============================================================
-- IFI KPI Calendar-Month Report — Aggregated by CY Year + Month + Managed
-- Used to populate OKR MonthlyUpdate rows in lib-okr-data RDS.
-- Produces two rows per (cy_year, cy_month): managed (1) and non-managed (0).
-- ============================================================
-- DROP required before CREATE OR REPLACE when column count changes in Redshift
DROP VIEW IF EXISTS public.lib_ifi_okr_monthly_report;
CREATE OR REPLACE VIEW public.lib_ifi_okr_monthly_report AS
SELECT
    cy_year,
    cy_month,
    managed_county,
    COUNT(*)                                                       AS total_surveyed,

    -- OKR 2.1: Correct & on-time incentive (3.1 survey-reported)
    SUM(correct_ontime)                                            AS n_correct_ontime,

    -- OKR 2.1: Correct & on-time incentive (FY26 method: objective amount + date)
    SUM(correct_ontime_fy26)                                       AS n_correct_ontime_fy26,

    -- OKR 2.3: Has CHC
    SUM(CAST(health_services_communityengagement_chc AS INT))      AS n_has_chc,

    -- OKR 2.4: Lifesaving commodities in stock
    SUM(lifesaving_instock)                                        AS n_lifesaving_instock,

    -- OKR 2.6: 2+ supervision visits
    SUM(CASE WHEN two_plusvisits = 1 THEN 1 ELSE 0 END)           AS n_two_plusvisits,
    COUNT(two_plusvisits)                                          AS d_two_plusvisits

FROM public.lib_ifi_community_data
GROUP BY cy_year, cy_month, managed_county;
