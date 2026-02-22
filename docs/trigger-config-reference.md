# Pipeline Trigger Configuration Reference

When triggering a DAG from the Airflow UI, you can pass runtime configuration as JSON via the **"Trigger DAG w/ config"** button. This JSON is accessible in the DAG as `dag_run.conf` and overrides YAML defaults for that single run.

---

## 1. Generic ETL Pipeline (`generic_etl_dag.py`)

Applies to all pipelines with `dag_type: generic` (e.g., `etl_lib_ifi_pipeline`).

### Supported Config

```json
{
  "mode": "full",
  "country": "liberia"
}
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mode` | `string` | From YAML `ingestion.mode` | `"full"` — reload all data (no watermark filter). `"incremental"` — only fetch records newer than the last watermark. |
| `country` | `string` | From YAML `source.config.default_country` | Override the country partition. Affects S3 path partitioning and can be used for multi-country pipelines. |

### Examples

**Full reload:**
```json
{
  "mode": "full"
}
```

**Incremental (default from YAML, but explicit):**
```json
{
  "mode": "incremental"
}
```

---

## 2. DHIS2 Pipeline (`dhis2_etl_dag.py`)

Applies to pipelines with `dag_type: dhis2` (e.g., `etl_lib_dhis2_pipeline`).

### Supported Config

```json
{
  "cdc_strategy_override": "rolling_window",
  "country": "liberia",
  "source_params": {
    "period": ["202601", "202602", "202603"],
    "dataSet": "dUjJetW69xZ",
    "orgUnit": "LHNiyIWuLdc",
    "children": true
  }
}
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `cdc_strategy_override` | `string` | From YAML `ingestion.cdc_strategy` | CDC strategy for this run. See **CDC Strategies** below. |
| `country` | `string` | From YAML `source.config.default_country` | Override the country partition. |
| `source_params` | `object` | `{}` | Runtime overrides for DHIS2 API query parameters. Merged on top of YAML `source.config.params`. See **Source Params** below. |

### CDC Strategies

| Strategy | How Periods Work | Filter | Best For |
|----------|-----------------|--------|----------|
| `full` | Uses the static `period` from YAML config (or `source_params` if provided) | None | Complete dataset refresh, one-time backfills |
| `incremental` | Ignores period entirely — scans all periods | `lastUpdated > watermark` | Daily scheduled runs picking up only new/changed records |
| `rolling_window` | Auto-generates last N months (from `rolling_window_months` in YAML, default 3) | None | Balances freshness with historical depth |

**Priority:** `cdc_strategy_override` (trigger config) > `ingestion.cdc_strategy` (YAML) > `ingestion.mode` (YAML fallback)

### Source Params

These override the DHIS2 API query parameters defined in `source.config.params` in the YAML config. Any DHIS2 API parameter can be passed here.

| Param | Type | Description |
|-------|------|-------------|
| `period` | `string` or `string[]` | DHIS2 period(s) to fetch. Single: `"202601"`. Multiple: `["202601", "202602"]`. Comma-separated string also works: `"202601,202602"`. **When provided, overrides CDC-computed periods.** |
| `dataSet` | `string` | DHIS2 dataset UID to fetch. |
| `orgUnit` | `string` | DHIS2 organisation unit UID. |
| `children` | `boolean` | Include child org units (`true`/`false`). |
| `lastUpdated` | `string` | ISO date filter — only records updated after this date. Typically set automatically by `incremental` CDC strategy. |

### Period Formats (DHIS2 API)

| Format | Example | Description |
|--------|---------|-------------|
| `YYYYMM` | `202601` | Monthly |
| `YYYY` | `2026` | Annual |
| `YYYYQn` | `2026Q1` | Quarterly |
| `YYYYWn` | `2026W5` | Weekly |
| `YYYYMMDD` | `20260115` | Daily |

### Examples

**Fetch specific months (overrides CDC strategy periods):**
```json
{
  "source_params": {
    "period": ["202501", "202502", "202503", "202504", "202505", "202506"]
  }
}
```

**Full backfill for all of 2025:**
```json
{
  "cdc_strategy_override": "full",
  "source_params": {
    "period": "2025"
  }
}
```

**Rolling window (last 6 months instead of default 3):**
This requires changing `rolling_window_months` in the YAML config. From the UI, you can instead pass the exact months:
```json
{
  "cdc_strategy_override": "full",
  "source_params": {
    "period": ["202509", "202510", "202511", "202512", "202601", "202602"]
  }
}
```

**Incremental from a specific date:**
```json
{
  "cdc_strategy_override": "incremental"
}
```
(Watermark is pulled automatically from DynamoDB — no need to pass `lastUpdated` manually.)

**Different dataset + org unit:**
```json
{
  "source_params": {
    "dataSet": "BfMAe6Itzgt",
    "orgUnit": "ImspTQPwCqd",
    "children": true,
    "period": ["202601", "202602"]
  }
}
```

**Override country partition:**
```json
{
  "country": "sierra_leone",
  "source_params": {
    "period": "202601"
  }
}
```

---

## 3. OKR Pipeline (`okr_etl_dag.py`)

The OKR pipeline does not ingest from an external source — it runs Redshift transforms and extracts views to RDS. It does not currently accept any trigger configuration.

---

## How to Trigger with Config

1. Navigate to the DAG in the Airflow UI
2. Click the **play button** (trigger) on the right
3. Select **"Trigger DAG w/ config"**
4. Paste valid JSON into the **Configuration JSON** field
5. Click **Trigger**

The `show_trigger_form_if_no_params` setting is enabled, so you will always see the config form when triggering.

---

## How CDC Strategy Interacts with Source Params

Understanding how `cdc_strategy_override` and `source_params.period` work together:

| Scenario | cdc_strategy | source_params.period | Result |
|----------|-------------|---------------------|--------|
| Default scheduled run | `rolling_window` (YAML) | not set | Auto-generates last 3 months |
| Override to full | `"full"` | not set | Uses static period from YAML (`"202601"`) |
| Override to incremental | `"incremental"` | not set | No period filter, uses `lastUpdated > watermark` |
| Custom periods | any | `["202501", "202502"]` | **User periods take priority** — CDC period logic is skipped |
| Full + custom period | `"full"` | `"2025"` | Fetches all of 2025 |
