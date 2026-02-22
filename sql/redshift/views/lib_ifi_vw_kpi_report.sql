-- ============================================================
-- IFI KPI Report — Monthly Level (one row per survey record)
-- ============================================================
CREATE OR REPLACE VIEW public.lib_ifi_vw_kpi_report AS
SELECT
    _id,
    county_label,
    managed_county,
    cy_year,
    cy_month,
    fy_year,
    fy_quarter,
    fy_month,
    CAST(fy_year AS VARCHAR) || '-' || CAST(fy_quarter AS VARCHAR) AS fy_year_quarter,

    -- KPI 3.1: Correct & on-time incentive (legacy)
    correct_ontime,

    -- KPI 3.4: Correct & on-time incentive (FY26 method)
    correct_incentive_amt,
    ontime_incentive_date,
    correct_ontime_fy26,

    -- KPI 3.5: 2+ supervision visits
    two_plusvisits,

    -- KPI 3.6: 1+ supervision visits
    one_plusvisits,

    -- KPI 3.7: Lifesaving commodities (any ACT variant)
    lifesaving_instock,

    -- KPI 3.7b: Lifesaving commodities (ACT AL only)
    lifesaving_instock_al,

    -- KPI 3.7c: Any ACT in stock
    supply_act_any_stock,

    -- KPI 3.8: All NCHAP commodities
    nchap_commodities,

    -- KPI 3.9: Clinical knowledge
    case_1_correct,
    case_2_correct,
    case_3_correct,
    correct_treatment,

    -- KPI 3.10: Community engagement
    CAST(health_services_communityengagement_chc AS INT) AS has_chc,
    chc_meetings_oneplus

FROM public.lib_ifi_community_data;
