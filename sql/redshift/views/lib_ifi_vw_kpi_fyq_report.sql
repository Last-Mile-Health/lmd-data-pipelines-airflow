-- ============================================================
-- IFI KPI FYQ Report — Aggregated by LMH Fiscal Year-Quarter
-- LMH FY: July–June (Jul-Sep=Q1, Oct-Dec=Q2, Jan-Mar=Q3, Apr-Jun=Q4)
-- ============================================================
CREATE OR REPLACE VIEW public.lib_ifi_vw_kpi_fyq_report AS
SELECT
    fy_year,
    fy_quarter,
    CAST(fy_year AS VARCHAR) || '-' || CAST(fy_quarter AS VARCHAR) AS fy_year_quarter,
    county_label,
    managed_county,
    COUNT(*) AS total_surveyed,

    -- KPI 3.1: Correct & on-time incentive (legacy)
    SUM(correct_ontime)                                            AS n_correct_ontime,
    ROUND(AVG(CAST(correct_ontime AS FLOAT)) * 100, 1)            AS pct_correct_ontime,

    -- KPI 3.4: Correct & on-time incentive (FY26 method)
    SUM(correct_ontime_fy26)                                       AS n_correct_ontime_fy26,
    ROUND(AVG(CAST(correct_ontime_fy26 AS FLOAT)) * 100, 1)       AS pct_correct_ontime_fy26,

    -- KPI 3.5: 2+ supervision visits
    SUM(CASE WHEN two_plusvisits = 1 THEN 1 ELSE 0 END)            AS n_two_plusvisits,
    COUNT(two_plusvisits)                                           AS d_two_plusvisits,
    ROUND(AVG(CAST(two_plusvisits AS FLOAT)) * 100, 1)             AS pct_two_plusvisits,

    -- KPI 3.6: 1+ supervision visits
    SUM(CASE WHEN one_plusvisits = 1 THEN 1 ELSE 0 END)            AS n_one_plusvisits,
    COUNT(one_plusvisits)                                           AS d_one_plusvisits,
    ROUND(AVG(CAST(one_plusvisits AS FLOAT)) * 100, 1)             AS pct_one_plusvisits,

    -- KPI 3.7: Lifesaving commodities (any ACT variant)
    SUM(lifesaving_instock)                                        AS n_lifesaving_instock,
    ROUND(AVG(CAST(lifesaving_instock AS FLOAT)) * 100, 1)        AS pct_lifesaving_instock,

    -- KPI 3.7b: Lifesaving commodities (ACT AL only)
    SUM(lifesaving_instock_al)                                     AS n_lifesaving_instock_al,
    ROUND(AVG(CAST(lifesaving_instock_al AS FLOAT)) * 100, 1)     AS pct_lifesaving_instock_al,

    -- KPI 3.7c: Any ACT in stock
    SUM(supply_act_any_stock)                                      AS n_supply_act_any,
    ROUND(AVG(CAST(supply_act_any_stock AS FLOAT)) * 100, 1)      AS pct_supply_act_any,

    -- KPI 3.8: All NCHAP commodities
    SUM(nchap_commodities)                                         AS n_nchap_commodities,
    ROUND(AVG(CAST(nchap_commodities AS FLOAT)) * 100, 1)         AS pct_nchap_commodities,

    -- KPI 3.9: Clinical knowledge — individual scenarios
    SUM(CASE WHEN case_1_correct = 1 THEN 1 ELSE 0 END)           AS n_case_1_correct,
    COUNT(case_1_correct)                                          AS d_case_1,
    ROUND(AVG(CAST(case_1_correct AS FLOAT)) * 100, 1)            AS pct_case_1_correct,

    SUM(CASE WHEN case_2_correct = 1 THEN 1 ELSE 0 END)           AS n_case_2_correct,
    COUNT(case_2_correct)                                          AS d_case_2,
    ROUND(AVG(CAST(case_2_correct AS FLOAT)) * 100, 1)            AS pct_case_2_correct,

    SUM(CASE WHEN case_3_correct = 1 THEN 1 ELSE 0 END)           AS n_case_3_correct,
    COUNT(case_3_correct)                                          AS d_case_3,
    ROUND(AVG(CAST(case_3_correct AS FLOAT)) * 100, 1)            AS pct_case_3_correct,

    -- KPI 3.9: All three correct
    SUM(correct_treatment)                                         AS n_correct_treatment,
    ROUND(AVG(CAST(correct_treatment AS FLOAT)) * 100, 1)         AS pct_correct_treatment,

    -- KPI 3.10: Has CHC
    SUM(CAST(health_services_communityengagement_chc AS INT))      AS n_has_chc,
    ROUND(AVG(CAST(health_services_communityengagement_chc AS FLOAT)) * 100, 1) AS pct_has_chc,

    -- KPI 3.10: CHC held 1+ meeting
    SUM(CASE WHEN chc_meetings_oneplus = 1 THEN 1 ELSE 0 END)     AS n_chc_meetings,
    COUNT(chc_meetings_oneplus)                                    AS d_chc_meetings,
    ROUND(AVG(CAST(chc_meetings_oneplus AS FLOAT)) * 100, 1)      AS pct_chc_meetings

FROM public.lib_ifi_community_data
GROUP BY fy_year, fy_quarter, county_label, managed_county;
