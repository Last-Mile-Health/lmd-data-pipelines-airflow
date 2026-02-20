-- Transform: vw_okr_example_1
-- Refreshes the OKR view with a snapshot date column.
-- Replace with your actual transform logic.

CREATE OR REPLACE VIEW public.vw_okr_example_1 AS
SELECT
    *,
    CURRENT_DATE AS snapshot_date
FROM public.okr_source_example_1;
