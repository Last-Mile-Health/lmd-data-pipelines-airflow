# DHIS2 Pipeline — Architecture & Operating Guide

This document covers the DHIS2 ETL pipeline (`config/pipelines/lib_dhis2_pipeline.yml`) and the changes shipped on `feature/dhis2-fixes`.

---

## 1. What's new on this branch

| Area | Change | Why |
|------|--------|-----|
| **Dimensions DAG** | New `etl_<pipeline>_dimensions` DAG (`dags/dhis2_dimensions_dag.py`) refreshes dim tables on its own cron, watermark-gated by `min_refresh_days`. | Dims rarely change. Re-pulling them on every fact run wasted DHIS2 API calls + Redshift COPY work. |
| **CDC strategies** | `full` now **requires** an explicit `period`; `rolling_window` auto-computes a list of last *N* months; `incremental` uses a `lastUpdated` cursor. | Prevents accidental unbounded pulls and makes back-fills explicit. |
| **Pagination** | Endpoint-aware: `/analytics` uses pager, `/dataValueSets` is single-shot, `/events` and `/trackedEntityInstances` use `totalPages=true`. Hard safety cap at 10,000 pages. | Old code looped on `pageCount` even for endpoints that don't paginate, causing duplicates / hangs. |
| **HTTP retries** | `requests.Session` with `urllib3.Retry` — retries 502/503/504/429 + connection resets without burning Airflow task retries. | DNS hiccups against the DHIS2 host were failing tasks. |
| **Body-read retries** | Wrapper around `session.get` retries `ChunkedEncodingError` / `ConnectionError` raised while reading the response body (3 attempts, exponential backoff). | `urllib3.Retry` only covers handshake/status; a server that returns 200 then drops mid-stream wasn't being retried. |
| **Period batching** | `/dataValueSets` requests are split into `ingestion.period_batch_size` periods per call (default 1). | Multi-period `/dataValueSets` responses overwhelmed the server mid-stream. |
| **Auth** | DHIS2 PATs use `Authorization: ApiToken …` (not `Bearer`). | Bearer never worked against DHIS2. |
| **Watermark** | `set_watermark` always bumps `last_successful_run`, but only overwrites `last_watermark_value` when a real max is observed. | Empty windows used to silently advance the cursor and skip late-arriving records. |
| **Dedup** | `raw_to_processed.py` supports a composite-hash dedup (`--dedup_key=_dedup_hash --dedup_columns=…`). | Required for `rolling_window` so 3-month re-pulls don't duplicate rows. |
| **Glue perf** | DataFrame is `persist()`-ed before multi-pass operations; redundant `df.count()` calls removed. | Each `count()` triggers a full scan. |
| **Curated SQL** | `lib_dhis2_pipeline_transform.sql` now strips `_pipeline_*` / `_dedup_hash` etc. — only DHIS2 source columns reach Redshift. | Keep curated tables clean. |
| **Quality checks** | New checks: `null_pct_max`, `freshness_max_hours`. | Catch silent partial loads. |
| **Dim consolidation** | Dropped `dhis2_dim_attribute_option` — both `categoryOptionCombo` and `attributeOptionCombo` resolve via `dhis2_dim_category_option` (same DHIS2 endpoint). | Eliminate duplicate API call. |
| **BI views** | New `lib_dhis2_orgunit_hierarchy`, `lib_cbis_datavalue` (names-only), and aggregate views by county/district. | BI consumers shouldn't see DHIS2 UIDs. |
| **Config caching** | `load_all_pipeline_configs()` is mtime-cached. | Scheduler heartbeats no longer re-parse YAML. |

---

## 2. Architecture

Two DAGs are generated per DHIS2 pipeline YAML.

### 2.1 Fact DAG — `etl_<pipeline_name>`

```
resolve_load_params
  → ingest_to_raw          (DHIS2 API → S3 raw JSON)
    → has_data             (short-circuit if zero records)
      → raw_to_processed   (Glue: clean / flatten / dedup / metadata cols)
        → processed_to_curated (Glue: curated SQL transforms)
          → ensure_crawler (create-once Glue crawler)
            → crawl_curated
              → load_to_redshift
                → update_watermark + run_quality_checks
```

`update_watermark` runs even on the skip path (`trigger_rule="none_failed"`), so successful empty windows still bump `last_successful_run`.

### 2.2 Dimensions DAG — `etl_<pipeline_name>_dimensions`

- Generated only when the YAML has `pipeline.dag_type: dhis2` **and** a non-empty `dimensions:` list.
- Schedule comes from `dimensions_refresh.cron` (default `0 6 1 * *` — monthly, 1st @ 06:00 UTC).
- Each dim is a fan-out task. Each task short-circuits if its DynamoDB watermark (`dim_watermark#<dim_name>`) is younger than `dimensions_refresh.min_refresh_days`.
- Failures don't block sibling dims.

---

## 3. CDC strategies

Set under `ingestion.cdc_strategy` in YAML (or override per run via `dag_run.conf.cdc_strategy_override`).

| Strategy | Behavior | When `period` comes from |
|----------|----------|--------------------------|
| `full` | Pulls everything inside the period(s). **Requires** `period` to be set. | YAML `source.config.params.period` **or** `dag_run.conf.source_params.period`. |
| `rolling_window` | Auto-computes the last `rolling_window_months` periods (`YYYYMM`) and passes them as repeated `period=` params. | Computed from `now()`. |
| `incremental` | Pulls everything updated since the stored `last_watermark_value`, sent to DHIS2 as `lastUpdated=`. | Not used — period is dropped. |

The fact YAML intentionally does **not** ship a `period` default — see the comment in `config/pipelines/lib_dhis2_pipeline.yml`.

---

## 4. How to run

### 4.1 On schedule

Both DAGs run automatically once enabled in Airflow. Defaults:

- Fact DAG: `schedule.cron` (weekly, currently Sunday morning).
- Dimensions DAG: `dimensions_refresh.cron` (monthly).

### 4.2 Manual trigger (UI)

Airflow UI → DAG → **Trigger DAG w/ config**:

```json
{
  "cdc_strategy_override": "rolling_window",
  "country": "liberia"
}
```

Common conf keys:

| Key | Type | Notes |
|-----|------|-------|
| `cdc_strategy_override` | `full` \| `rolling_window` \| `incremental` | Overrides YAML for this run only. |
| `country` | string | Defaults to `source.config.default_country`. Used in S3 path + watermark. |
| `source_params.period` | string or list | CSV (`"202401,202402"`) or JSON array. Required for `full`. |
| `source_params.dataSet` / `source_params.orgUnit` | string | Override YAML defaults. |

### 4.3 Backfill — Jan 2024 → now

DHIS2 monthly periods are `YYYYMM`. Pass an explicit list with `cdc_strategy_override: "full"`:

```json
{
  "cdc_strategy_override": "full",
  "source_params": {
    "period": "202401,202402,202403,202404,202405,202406,202407,202408,202409,202410,202411,202412,202501,202502,202503,202504,202505,202506,202507,202508,202509,202510,202511,202512,202601,202602,202603,202604"
  }
}
```

CLI equivalent:

```bash
airflow dags trigger etl_lib_dhis2_pipeline \
  --conf '{"cdc_strategy_override":"full","source_params":{"period":"202401,...,202604"}}'
```

The ingestor automatically splits this into `ingestion.period_batch_size` periods per `/dataValueSets` call (default `1`), so a 28-period backfill becomes 28 small requests rather than one giant one. If your DHIS2 instance is healthy enough to stream multi-period responses, raise `period_batch_size` in the YAML (e.g. `6` or `12`) to cut request count.

### 4.4 Force-refresh a single dimension

The dimensions DAG short-circuits per dim. To force a refresh, delete that dim's DynamoDB watermark row, then trigger `etl_<pipeline>_dimensions`:

```bash
aws dynamodb delete-item \
  --table-name <metadata_table> \
  --key '{"pipeline_name":{"S":"lib_dhis2_pipeline"},"execution_id":{"S":"dim_watermark#dhis2_dim_orgunit"}}'

airflow dags trigger etl_lib_dhis2_pipeline_dimensions
```

Or temporarily set `dimensions_refresh.min_refresh_days: 0` in YAML.

---

## 5. YAML reference (DHIS2-specific)

```yaml
pipeline:
  dag_type: dhis2                         # triggers fact + dimensions DAG factories

source:
  type: dhis2
  secret_name: <secrets-manager-name>     # must contain DHIS2_BASE_URL, DHIS2_USERNAME/PASSWORD or DHIS2_TOKEN
  config:
    base_url: https://...
    endpoint: /dataValueSets              # see pagination matrix in §1
    auth_type: basic | token              # token = PAT (sent as ApiToken)
    params:
      dataSet: "..."
      orgUnit: "..."
      children: true
      # period intentionally omitted — see CDC strategies
    page_size: 1000
    default_country: liberia

ingestion:
  cdc_strategy: rolling_window            # full | rolling_window | incremental
  rolling_window_months: 3
  incremental_key: lastUpdated            # DHIS2 field used as cursor
  period_batch_size: 1                    # /dataValueSets periods per request (raise if server is healthy)

processed:
  glue_job_script: glue_jobs/lib_dhis2_pipeline/raw_to_processed.py
  custom_sql: sql/raw_to_processed/lib_dhis2_pipeline_clean.sql
  deduplicate_key: _dedup_hash            # composite-hash mode
  deduplicate_columns: "dataelement,period,orgunit,categoryoptioncombo,attributeoptioncombo"

dimensions_refresh:
  cron: "0 6 1 * *"
  min_refresh_days: 25                    # skip dim if last refresh < N days

dimensions:
  - name: dhis2_dim_dataelement
    endpoint: /dataElements
    response_key: dataElements
    fields: "id,displayName,shortName"
    table: dhis2_dim_dataelement
    schema: public
    columns: [...]

quality_checks:
  - { type: row_count_min, threshold: 1000 }
  - { type: null_pct_max, column: dataelement, threshold: 0.01 }
  - { type: freshness_max_hours, column: _ingestion_timestamp, max_hours: 192 }

schedule:
  cron: "0 6 * * 0"                       # Sunday 06:00 UTC
  catchup: false
  retries: 2
  retry_delay_minutes: 5
  timeout_minutes: 120
```

---

## 6. BI views (Redshift)

Run the view file once after the curated table exists:

```bash
psql … -f sql/redshift/views/lib_dhis2_vw_bi_datavalue.sql
```

It is idempotent (uses `CREATE OR REPLACE VIEW` and `IF EXISTS` for cleanup). Provides:

| View | Grain | Purpose |
|------|-------|---------|
| `lib_dhis2_orgunit_hierarchy` | one row per org unit | Resolves DHIS2 path → county / district / facility names. Adjust `SPLIT_PART` indexes if your hierarchy isn't `Country/County/District/Facility`. |
| `lib_cbis_datavalue` | one row per data value | Names-only detail view (no DHIS2 UIDs). |
| `lib_cbis_datavalue_by_county` | county × data element × period | Aggregated counts + numeric sum/avg. |
| `lib_cbis_datavalue_by_district` | district × data element × period | Same, district grain. |

Numeric aggregation guards with a regex (`^-?[0-9]+(\.[0-9]+)?$`) since DHIS2 stores all values as text.

---

## 7. Operational notes

- **Watermarks** live in the DynamoDB metadata table under partition key `pipeline_name`:
  - `execution_id = "watermark"` — fact pipeline
  - `execution_id = "dim_watermark#<dim_name>"` — per-dimension
- **Glue console URLs** logged on `raw_to_processed` / `processed_to_curated` task start use the working `?region=…#/job/<name>/runs` pattern (Airflow's built-in URL 404s on some regions).
- **Deferrable Glue ops** are currently `deferrable=False` on MWAA — the triggerer wasn't completing tasks even after Glue success. Re-enable once triggerer logs/IAM are confirmed.
- **Empty windows** still bump `last_successful_run` so on-call dashboards don't show a false "stalled pipeline."
