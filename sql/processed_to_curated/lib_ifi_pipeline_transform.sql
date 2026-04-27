-- IFI Processed → Curated business logic SQL
-- Applied by Glue Spark using the transform step convention.
-- Each step reads from __TABLE__ and produces a new __TABLE__.
-- Dates arrive pre-formatted from the processed layer (ISO 8601 / TIMESTAMP).
-- No datetime conversion required.

-- ============================================================
-- KPI REFERENCE GUIDE
-- ============================================================
-- All KPIs are binary (1 = condition met, 0 = not met) unless
-- noted. NULL is preserved where the source field is missing,
-- indicating the question was not asked / not applicable.
--
-- KPI 3.1 | correct_ontime
--   Whether a CHA received their incentive payment that was
--   both the correct amount AND paid on time. This is a simple
--   composite of the two supervisor-reported flags captured
--   directly on the IFI survey form. Both conditions must be
--   true for the CHA to be counted as receiving a correct and
--   on-time payment.
--
-- KPI 3.4a | correct_incentive_amt  [FY26 method]
--   Whether the CHA's reported incentive payment equalled
--   exactly USD 70. Payments reported in Liberian Dollars (LD)
--   are converted to USD using the fixed exchange rate of
--   0.0055 (1 LD = 0.0055 USD) before comparison. Currency is
--   indicated by: 1 = USD, 2 = LD.
--
-- KPI 3.4b | ontime_incentive_date  [FY26 method]
--   Whether the incentive payment date fell within the defined
--   on-time window of the 5th–10th of the month. Two scenarios
--   are considered on-time:
--     (a) Payment between 5th–10th of the same month as the
--         survey date.
--     (b) If the survey is conducted before the 11th of the
--         month, a payment between the 5th–10th of the prior
--         month is also considered on-time (i.e. the prior
--         month's payment window has not yet closed). The
--         December/January year-boundary is handled explicitly.
--
-- KPI 3.4c | correct_ontime_fy26   [FY26 method]
--   FY26 composite of correct_incentive_amt AND
--   ontime_incentive_date. Replaces the survey-reported flags
--   used in KPI 3.1 with objectively calculated values based
--   on the actual payment amount and date recorded on the form.
--
-- KPI 3.5 | two_plusvisits
--   Whether the CHA received 2 or more supervision visits in
--   the 4 weeks prior to the survey. Minimum recommended
--   supervision frequency under the NCHAP programme is 2
--   visits per month. NULL if the supervision visit count is
--   missing.
--
-- KPI 3.6 | one_plusvisits
--   Whether the CHA received at least 1 supervision visit in
--   the 4 weeks prior to the survey. A lower-bound check used
--   alongside KPI 3.5 to track CHAs receiving any supervision
--   at all. NULL if the supervision visit count is missing.
--
-- KPI 3.7 | lifesaving_instock
--   Whether the CHA had all life-saving commodities in stock
--   at the time of the visit. The commodity set is defined as:
--     - ACT (either ACT AL20 OR both ACT25 + ASAQ50 in stock)
--     - Amoxicillin
--     - ORS
--     - Zinc
--   The OR condition on ACT type accommodates facilities that
--   stock AL-formulation versus ASAQ-formulation artemisinin.
--
-- KPI 3.7b | lifesaving_instock_al
--   Stricter variant of KPI 3.7 requiring ACT AL20 specifically
--   (rather than ASAQ as an alternative). Used for sites or
--   reporting periods where only AL supply is tracked.
--
-- KPI 3.7c | supply_act_any_stock
--   Helper flag indicating whether any ACT formulation is in
--   stock (ACT AL20 OR [ACT25 AND ASAQ50]). Used as a
--   standalone commodity availability indicator and as an
--   input to the lifesaving composite.
--
-- KPI 3.8 | nchap_commodities
--   Whether the CHA had ALL 14 NCHAP-defined commodities in
--   stock simultaneously. This is the broadest commodity
--   check, covering the full NCHAP supply kit:
--   Microlut, Microgynon, Male condoms, Female condoms,
--   Disposable gloves, ACT25, ASAQ50, Artesunate,
--   Amoxicillin, ORS, Zinc, PCM, MUAC tape, Dispatch bag.
--
-- KPI 3.9a | case_1_correct / case_2_correct / case_3_correct
--   Whether the CHA selected the correct answer for each of
--   three clinical case scenarios presented during the IFI
--   supervision visit (FY25 scenarios):
--     Scenario 1 – Diarrhea (2yr old, 15 days watery stool,
--       no fever): correct = refer to facility with ORS (ans 3)
--     Scenario 2 – Pneumonia (3mo old, fast breathing 60 bpm,
--       no danger signs): correct = Amox 250mg 2x/day 5 days
--       (ans 4)
--     Scenario 3 – Pregnancy (swollen face/hands): correct =
--       identify danger sign and refer to facility (ans 2)
--   NULL is preserved when the scenario was not administered.
--
-- KPI 3.9b | correct_treatment
--   Composite: CHA answered ALL three case scenarios correctly.
--   A CHA must score 3/3 to be counted as having correct
--   treatment knowledge. Any single incorrect or missing
--   response results in 0.
--
-- KPI 3.10 | chc_meetings_oneplus
--   Of communities that have a Community Health Committee (CHC),
--   whether the CHC held at least one meeting in the past
--   month. Reported as the count of meetings; this flag
--   converts that to a binary indicator (>0 = 1). NULL when
--   the meetings field was not completed.
--
-- supply_actal20_so_stock_clean
--   Free-text imputed version of stock_info_supply_actal20_so_stock.
--   Where the raw stock field is NULL, the supervisor's open-text
--   notes (txt_notes) are parsed for known phrases indicating AL20
--   presence or absence. Positive and negative patterns are drawn
--   directly from the Stata DQA script. Remaining NULLs (no raw
--   value and no matching text) are NOT further imputed here —
--   multiple imputation (Stata mi impute logit) is not replicable
--   in SQL and must be applied offline if needed.
--   lifesaving_instock_al uses this cleaned value.
--
-- ip | Implementing Partner
--   County-to-partner mapping (time-aware from Jan 2026):
--     IRC    — Bong(2), Grand Kru(7), Lofa(8), River Gee(14)
--     LMH    — Rivercess(13) [all time]
--              Grand Bassa(4) [before 2026-01-01 only; transferred Jan 2026]
--     CHT/WB — Gbarpolu(3), Grand Cape Mount(5), Grand Gedeh(6)
--     Plan   — Bomi(1), Margibi(9), Maryland(10), Montserrado(11),
--               Nimba(12), Sinoe(15)
--
-- Row filter: basic_info_data_collector_org IN (1, 3)
--   Restricts to county-level supervision visits only (orgs 1 and 3).
--   National-level visits are excluded from the curated layer,
--   matching the Stata analysis scope.


-- ============================================================
-- transform_business_logic
-- Apply IFI-specific KPI business rules
-- ============================================================
SELECT
    *,

    -- --------------------------------------------------------
    -- KPI 3.1: correct_ontime
    -- CHA received correct AND on-time incentive payment
    -- Source fields: grp_incentive1_incentive_correct,
    --                grp_incentive1_incentive_ontime
    -- --------------------------------------------------------
    CASE
        WHEN CAST(grp_incentive1_incentive_correct AS INT) = 1
         AND CAST(grp_incentive1_incentive_ontime  AS INT) = 1 THEN 1
        ELSE 0
    END AS correct_ontime,

    -- --------------------------------------------------------
    -- KPI 3.4 (FY26): correct_incentive_amt
    -- Incentive amount equals USD 70, converting LD → USD
    -- (currency: 1=USD, 2=LD; exchange rate 0.0055)
    -- Source fields: grp_incentive2_incentive_amt,
    --                grp_incentive2_incentive_currency
    -- --------------------------------------------------------
    CASE
        WHEN CAST(grp_incentive2_incentive_currency AS INT) = 1
         AND CAST(grp_incentive2_incentive_amt AS DOUBLE) = 70.0 THEN 1
        WHEN CAST(grp_incentive2_incentive_currency AS INT) = 2
         AND ROUND(CAST(grp_incentive2_incentive_amt AS DOUBLE) * 0.0055, 2) = 70.0 THEN 1
        ELSE 0
    END AS correct_incentive_amt,

    -- --------------------------------------------------------
    -- KPI 3.4 (FY26): ontime_incentive_date
    -- Payment occurred between 5th–10th of the month.
    -- Either in the same month as the survey, or in the prior
    -- month if the survey date is still before the 11th.
    -- Source fields: grp_incentive1_incentive_date (string 'YYYY-MM-DD'),
    --                manual_date (pre-formatted timestamp)
    -- --------------------------------------------------------
    CASE
        -- Payment between 5th–10th of the same survey month
        WHEN DAY(TO_DATE(grp_incentive1_incentive_date))   BETWEEN 5 AND 10
         AND MONTH(TO_DATE(grp_incentive1_incentive_date)) = MONTH(TO_DATE(manual_date)) THEN 1
        -- Survey is still before the 11th → prior-month window also counts
        WHEN DAY(TO_DATE(manual_date)) < 11
         AND DAY(TO_DATE(grp_incentive1_incentive_date)) BETWEEN 5 AND 10
         AND (
               MONTH(TO_DATE(grp_incentive1_incentive_date)) = MONTH(TO_DATE(manual_date)) - 1
            OR (MONTH(TO_DATE(grp_incentive1_incentive_date)) = 12
                AND MONTH(TO_DATE(manual_date)) = 1)
             ) THEN 1
        ELSE 0
    END AS ontime_incentive_date,

    -- --------------------------------------------------------
    -- KPI 3.4 (FY26): correct_ontime_fy26
    -- Correct amount AND on-time date (FY26 method)
    -- Inlined because Spark SQL cannot reference same-query aliases
    -- --------------------------------------------------------
    CASE
        WHEN (
            CASE
                WHEN CAST(grp_incentive2_incentive_currency AS INT) = 1
                 AND CAST(grp_incentive2_incentive_amt AS DOUBLE) = 70.0 THEN 1
                WHEN CAST(grp_incentive2_incentive_currency AS INT) = 2
                 AND ROUND(CAST(grp_incentive2_incentive_amt AS DOUBLE) * 0.0055, 2) = 70.0 THEN 1
                ELSE 0
            END = 1
        ) AND (
            CASE
                WHEN DAY(TO_DATE(grp_incentive1_incentive_date))   BETWEEN 5 AND 10
                 AND MONTH(TO_DATE(grp_incentive1_incentive_date)) = MONTH(TO_DATE(manual_date)) THEN 1
                WHEN DAY(TO_DATE(manual_date)) < 11
                 AND DAY(TO_DATE(grp_incentive1_incentive_date)) BETWEEN 5 AND 10
                 AND (
                       MONTH(TO_DATE(grp_incentive1_incentive_date)) = MONTH(TO_DATE(manual_date)) - 1
                    OR (MONTH(TO_DATE(grp_incentive1_incentive_date)) = 12
                        AND MONTH(TO_DATE(manual_date)) = 1)
                     ) THEN 1
                ELSE 0
            END = 1
        ) THEN 1
        ELSE 0
    END AS correct_ontime_fy26,

    -- --------------------------------------------------------
    -- KPI 3.5: two_plusvisits
    -- CHA received 2+ supervision visits in last 4 weeks
    -- Source field: supervision_grp_supervision_last4wks (string)
    -- --------------------------------------------------------
    CASE
        WHEN CAST(supervision_grp_supervision_last4wks AS INT) >= 2 THEN 1
        WHEN CAST(supervision_grp_supervision_last4wks AS INT) <  2 THEN 0
        ELSE NULL
    END AS two_plusvisits,

    -- --------------------------------------------------------
    -- KPI 3.6: one_plusvisits
    -- CHA received 1+ supervision visits in last 4 weeks
    -- Source field: supervision_grp_supervision_last4wks (string)
    -- --------------------------------------------------------
    CASE
        WHEN CAST(supervision_grp_supervision_last4wks AS INT) >= 1 THEN 1
        WHEN CAST(supervision_grp_supervision_last4wks AS INT) <  1 THEN 0
        ELSE NULL
    END AS one_plusvisits,

    -- --------------------------------------------------------
    -- KPI 3.7: lifesaving_instock
    -- All lifesaving commodities in stock:
    --   (ACT AL20 OR (ACT25 AND ASAQ50)) AND Amox AND ORS AND Zinc
    -- Source fields: all string '0'/'1'
    -- --------------------------------------------------------
    CASE
        WHEN (
                  CAST(stock_info_supply_actal20_so_stock AS INT) = 1
               OR (CAST(stock_info_supply_act25_stock    AS INT) = 1
                   AND CAST(stock_info_supply_actasaq50_stock AS INT) = 1)
             )
         AND CAST(stock_info_supply_amox_stock AS INT) = 1
         AND CAST(stock_info_supply_ors_stock  AS INT) = 1
         AND CAST(stock_info_supply_zinc_stock AS INT) = 1 THEN 1
        ELSE 0
    END AS lifesaving_instock,

    -- --------------------------------------------------------
    -- KPI 3.7b: lifesaving_instock_al
    -- All lifesaving commodities using ACT AL only.
    -- Uses free-text imputed AL20 value (supply_actal20_so_stock_clean)
    -- inlined here because Spark SQL cannot reference same-query aliases.
    -- --------------------------------------------------------
    CASE
        WHEN (
            CASE
                WHEN CAST(stock_info_supply_actal20_so_stock AS INT) IS NOT NULL
                    THEN CAST(stock_info_supply_actal20_so_stock AS INT)
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has act( al)%'                           THEN 1
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has act(al)%'                            THEN 1
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has act (al)%'                           THEN 1
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has al not%'                             THEN 1
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%act( al) in stock not asaq%'                 THEN 1
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%al is available with cha%'                   THEN 1
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has in stock act(al) not asaq%'          THEN 1
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has al in stock not asaq%'               THEN 1
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%stock out of al in community%'               THEN 0
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%no malaria tablet of all age group%'         THEN 0
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%no iccm commodities%'                        THEN 0
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%no drugs for cha%'                           THEN 0
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%no drugs has been delivered to cha%'         THEN 0
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%stock out of iccm commodities%'              THEN 0
                WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%only ors, zinc and amoxicillin are in stock%' THEN 0
                ELSE NULL
            END
        ) = 1
         AND CAST(stock_info_supply_amox_stock AS INT) = 1
         AND CAST(stock_info_supply_ors_stock  AS INT) = 1
         AND CAST(stock_info_supply_zinc_stock AS INT) = 1 THEN 1
        ELSE 0
    END AS lifesaving_instock_al,

    -- --------------------------------------------------------
    -- KPI 3.7c: supply_act_any_stock
    -- ACT AL OR ASAQ in stock (composite)
    -- --------------------------------------------------------
    CASE
        WHEN CAST(stock_info_supply_actal20_so_stock AS INT) = 1
          OR (CAST(stock_info_supply_act25_stock    AS INT) = 1
              AND CAST(stock_info_supply_actasaq50_stock AS INT) = 1) THEN 1
        ELSE 0
    END AS supply_act_any_stock,

    -- --------------------------------------------------------
    -- KPI 3.8: nchap_commodities
    -- All NCHAP commodities in stock
    -- Source fields: all string '0'/'1'
    -- --------------------------------------------------------
    CASE
        WHEN CAST(stock_info_supplymicrolut_stock          AS INT) = 1
         AND CAST(stock_info_supplymicrogynon_stock         AS INT) = 1
         AND CAST(stock_info_supplymcondom_stock            AS INT) = 1
         AND CAST(stock_info_supply_fcondom_stock           AS INT) = 1
         AND CAST(stock_info_supply_disposable_gloves_stock AS INT) = 1
         AND CAST(stock_info_supply_act25_stock             AS INT) = 1
         AND CAST(stock_info_supply_actasaq50_stock         AS INT) = 1
         AND CAST(stock_info_supply_artesunate_stock        AS INT) = 1
         AND CAST(stock_info_supply_amox_stock              AS INT) = 1
         AND CAST(stock_info_supply_ors_stock               AS INT) = 1
         AND CAST(stock_info_supply_zinc_stock              AS INT) = 1
         AND CAST(stock_info_supply_pcm_stock               AS INT) = 1
         AND CAST(stock_info_supplymuac_stock               AS INT) = 1
         AND CAST(stock_info_supply_dispbag_stock           AS INT) = 1 THEN 1
        ELSE 0
    END AS nchap_commodities,

    -- --------------------------------------------------------
    -- KPI 3.9: Case scenario correct responses (FY25)
    -- Scenario 1 – Diarrhea:   correct answer = '3'
    -- Scenario 2 – Pneumonia:  correct answer = '4'
    -- Scenario 3 – Pregnancy:  correct answer = '2'
    -- Source fields: sd_scenario_1/2/3 (string)
    -- --------------------------------------------------------
    CASE
        WHEN sd_scenario_1 IS NULL THEN NULL
        WHEN CAST(sd_scenario_1 AS INT) = 3 THEN 1
        ELSE 0
    END AS case_1_correct,

    CASE
        WHEN sd_scenario_2 IS NULL THEN NULL
        WHEN CAST(sd_scenario_2 AS INT) = 4 THEN 1
        ELSE 0
    END AS case_2_correct,

    CASE
        WHEN sd_scenario_3 IS NULL THEN NULL
        WHEN CAST(sd_scenario_3 AS INT) = 2 THEN 1
        ELSE 0
    END AS case_3_correct,

    -- --------------------------------------------------------
    -- KPI 3.9: correct_treatment
    -- All three case scenarios answered correctly
    -- --------------------------------------------------------
    CASE
        WHEN (CASE WHEN sd_scenario_1 IS NULL THEN NULL
                   WHEN CAST(sd_scenario_1 AS INT) = 3 THEN 1 ELSE 0 END) = 1
         AND (CASE WHEN sd_scenario_2 IS NULL THEN NULL
                   WHEN CAST(sd_scenario_2 AS INT) = 4 THEN 1 ELSE 0 END) = 1
         AND (CASE WHEN sd_scenario_3 IS NULL THEN NULL
                   WHEN CAST(sd_scenario_3 AS INT) = 2 THEN 1 ELSE 0 END) = 1
        THEN 1
        ELSE 0
    END AS correct_treatment,

    -- --------------------------------------------------------
    -- KPI 3.10: chc_meetings_oneplus
    -- Had at least one CHC meeting in the last month
    -- Source field: health_services_communityengagement_meetings (string)
    -- --------------------------------------------------------
    CASE
        WHEN health_services_communityengagement_meetings IS NULL THEN NULL
        WHEN CAST(health_services_communityengagement_meetings AS INT) > 0  THEN 1
        ELSE 0
    END AS chc_meetings_oneplus,

    -- --------------------------------------------------------
    -- supply_actal20_so_stock_clean
    -- AL20 stock status with free-text imputation for NULLs.
    -- Raw value is used when present; txt_notes is parsed otherwise.
    -- --------------------------------------------------------
    CASE
        WHEN CAST(stock_info_supply_actal20_so_stock AS INT) IS NOT NULL
            THEN CAST(stock_info_supply_actal20_so_stock AS INT)
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has act( al)%'                           THEN 1
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has act(al)%'                            THEN 1
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has act (al)%'                           THEN 1
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has al not%'                             THEN 1
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%act( al) in stock not asaq%'                 THEN 1
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%al is available with cha%'                   THEN 1
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has in stock act(al) not asaq%'          THEN 1
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%cha has al in stock not asaq%'               THEN 1
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%stock out of al in community%'               THEN 0
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%no malaria tablet of all age group%'         THEN 0
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%no iccm commodities%'                        THEN 0
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%no drugs for cha%'                           THEN 0
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%no drugs has been delivered to cha%'         THEN 0
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%stock out of iccm commodities%'              THEN 0
        WHEN LOWER(COALESCE(txt_notes, '')) LIKE '%only ors, zinc and amoxicillin are in stock%' THEN 0
        ELSE NULL
    END AS supply_actal20_so_stock_clean,

    -- --------------------------------------------------------
    -- ip: Implementing Partner
    -- Derived from county code (basic_info_county / managed_county logic)
    -- --------------------------------------------------------
    CASE
        WHEN CAST(basic_info_county AS INT) IN (2, 7, 8, 14)          THEN 'IRC'
        WHEN CAST(basic_info_county AS INT) = 13                        THEN 'LMH'
        WHEN CAST(basic_info_county AS INT) = 4
         AND manual_date < TO_TIMESTAMP('2026-01-01')                  THEN 'LMH'
        WHEN CAST(basic_info_county AS INT) IN (3, 5, 6)               THEN 'CHT/WB'
        WHEN CAST(basic_info_county AS INT) IN (1, 9, 10, 11, 12, 15) THEN 'Plan'
        ELSE NULL
    END AS ip

FROM __TABLE__
WHERE CAST(basic_info_data_collector_org AS INT) IN (1, 3);  -- county-level supervision only
