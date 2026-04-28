"""
Dedicated DHIS2 ETL DAG — fact pipeline only.

Flow:
    resolve_load_params
        → ingest_to_raw
            → has_data (short-circuit for empty rolling/full pulls)
                → raw_to_processed (Glue, deferrable)
                    → processed_to_curated (Glue, deferrable)
                        → ensure_crawler (create-once, NEW_FOLDERS_ONLY)
                            → crawl_curated (GlueCrawlerOperator, deferrable)
                                → load_to_redshift
                                    → post_load (update_watermark + quality_checks)

Dimensions are loaded by a separate DAG (etl_<pipeline>_dimensions) on its own
schedule with per-dim watermark gating — they hardly change, no need to refresh
on every fact run.
"""
import json
import logging
import uuid
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.email import send_email
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

from utils.config_loader import load_all_pipeline_configs, get_env_config, build_flow_tags

log = logging.getLogger(__name__)


def _log_glue_console_url(job_name: str, region: str):
    """Return an on_execute_callback that logs a working Glue Studio URL.

    Airflow's GlueJobOperator emits a URL pattern that sometimes 404s
    (region in path, no ?from=monitoring). This pattern always works:
    region as subdomain + /runs page, so the user clicks into the latest run.
    """
    def _cb(context):
        ti = context["task_instance"]
        url = (
            f"https://{region}.console.aws.amazon.com/gluestudio/home"
            f"?region={region}#/job/{job_name}/runs"
        )
        ti.log.info(f"[Glue] Console (working URL): {url}")
    return _cb


def _on_failure_callback(context, recipients, pipeline_name):
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    send_email(
        to=recipients,
        subject=f"[FAILED] Pipeline: {pipeline_name} | Task: {task_instance.task_id}",
        html_content=f"""
        <h3>Pipeline Failure Alert</h3>
        <p><b>Pipeline:</b> {pipeline_name}</p>
        <p><b>Task:</b> {task_instance.task_id}</p>
        <p><b>Execution Date:</b> {context.get('execution_date')}</p>
        <p><b>Try Number:</b> {task_instance.try_number}</p>
        <p><b>Error:</b></p>
        <pre>{exception}</pre>
        <p><b>Log URL:</b> <a href="{task_instance.log_url}">{task_instance.log_url}</a></p>
        """,
    )


def _on_start_callback(context, recipients, pipeline_name):
    dag_run = context.get("dag_run")
    run_type = getattr(dag_run, "run_type", "unknown")
    trigger_label = "Manual Trigger" if run_type == "manual" else "Scheduled"
    conf = getattr(dag_run, "conf", None) or {}
    conf_html = (
        f"<pre>{json.dumps(conf, indent=2)}</pre>"
        if conf
        else "<p><i>No run config provided (defaults from YAML will be used)</i></p>"
    )
    send_email(
        to=recipients,
        subject=f"[STARTED] Pipeline: {pipeline_name}",
        html_content=f"""
        <h3>Pipeline Started</h3>
        <p><b>Pipeline:</b> {pipeline_name}</p>
        <p><b>Trigger:</b> {trigger_label}</p>
        <p><b>Execution Date:</b> {context.get('execution_date')}</p>
        <p><b>Run ID:</b> {context.get('run_id')}</p>
        <h4>Run Config</h4>
        {conf_html}
        """,
    )


def _on_success_callback(context, recipients, pipeline_name):
    send_email(
        to=recipients,
        subject=f"[SUCCESS] Pipeline: {pipeline_name}",
        html_content=f"""
        <h3>Pipeline Completed Successfully</h3>
        <p><b>Pipeline:</b> {pipeline_name}</p>
        <p><b>Execution Date:</b> {context.get('execution_date')}</p>
        <p><b>Run ID:</b> {context.get('run_id')}</p>
        """,
    )


def create_dhis2_dag(pipeline_name: str, config: dict):
    env = get_env_config()
    schedule_cfg = config.get("schedule", {})
    alerts_cfg = config.get("alerts", {})
    email_recipients = alerts_cfg.get("email_recipients", [])

    default_args = {
        "owner": config["pipeline"].get("owner", "data-team"),
        "retries": schedule_cfg.get("retries", 2),
        "retry_delay": timedelta(minutes=schedule_cfg.get("retry_delay_minutes", 5)),
        "execution_timeout": timedelta(minutes=schedule_cfg.get("timeout_minutes", 120)),
    }

    dag_callbacks = {}
    if email_recipients:
        if alerts_cfg.get("email_on_failure", True):
            default_args["on_failure_callback"] = lambda ctx: _on_failure_callback(ctx, email_recipients, pipeline_name)
        if alerts_cfg.get("email_on_success", True):
            dag_callbacks["on_success_callback"] = lambda ctx: _on_success_callback(ctx, email_recipients, pipeline_name)
        # NOTE: do NOT set default_args["email"]/email_on_failure — that triggers
        # Airflow's built-in SMTP on top of our custom callback (double email).

    @dag(
        dag_id=f"etl_{pipeline_name}",
        default_args=default_args,
        description=config["pipeline"].get("description", ""),
        schedule=schedule_cfg.get("cron"),
        start_date=datetime(2026, 1, 1),
        catchup=schedule_cfg.get("catchup", False),
        tags=config["pipeline"].get("tags", []) + build_flow_tags(config),
        max_active_runs=1,
        doc_md=f"### DHIS2 Pipeline: `{pipeline_name}`\n\n{config['pipeline'].get('description', '')}",
        **dag_callbacks,
    )
    def dhis2_pipeline():

        # ── TASK 1: Resolve load params + pre-compute deterministic S3 paths ──
        _start_cb = None
        if email_recipients and alerts_cfg.get("email_on_start", False):
            _start_cb = lambda ctx: _on_start_callback(ctx, email_recipients, pipeline_name)

        @task(on_execute_callback=_start_cb)
        def resolve_load_params(**context):
            from dateutil.relativedelta import relativedelta
            from utils.cdc import get_watermark, compute_boundaries

            conf = context["dag_run"].conf or {}
            ingestion_cfg = config["ingestion"]
            cdc_strategy = conf.get(
                "cdc_strategy_override",
                ingestion_cfg.get("cdc_strategy", ingestion_cfg.get("mode", "full")),
            )

            execution_id = str(uuid.uuid4())
            ingestion_time = datetime.utcnow().isoformat()
            country = conf.get("country", config["source"]["config"].get("default_country", "global"))

            params = {
                "execution_id": execution_id,
                "ingestion_time": ingestion_time,
                "cdc_strategy": cdc_strategy,
                "pipeline_name": pipeline_name,
                "country": country,
            }

            source_params = conf.get("source_params", {}) or {}
            if source_params:
                # Avoid mutating dag_run.conf
                source_params = dict(source_params)
                params["source_params"] = source_params
                if "period" in source_params:
                    user_periods = source_params["period"]
                    if isinstance(user_periods, str):
                        user_periods = [p.strip() for p in user_periods.split(",") if p.strip()]
                    source_params["period"] = user_periods
                    params["period_list"] = user_periods

            if cdc_strategy == "incremental":
                params["mode"] = "incremental"
                params["incremental_key"] = ingestion_cfg.get("incremental_key")
                watermark = get_watermark(
                    pipeline_name=pipeline_name,
                    table_name=env["metadata_table"],
                )
                params.update(compute_boundaries(
                    watermark=watermark,
                    incremental_key=ingestion_cfg.get("incremental_key"),
                ))
            elif cdc_strategy == "rolling_window":
                params["mode"] = "rolling_window"
                window_months = ingestion_cfg.get("rolling_window_months", 3)
                now = datetime.utcnow()
                params["period_list"] = [
                    (now - relativedelta(months=i)).strftime("%Y%m")
                    for i in range(window_months)
                ]
            else:
                params["mode"] = "full"

            # Pre-compute deterministic S3 output paths so deferrable Glue ops can reference them.
            now_dt = datetime.fromisoformat(ingestion_time)
            date_path = now_dt.strftime("%Y/%m/%d")
            key_suffix = f"{country}/{pipeline_name}/{date_path}/{execution_id}/"
            params["processed_s3_path"] = f"s3://{env['processed_bucket']}/{key_suffix}"
            params["curated_s3_path"] = f"s3://{env['curated_bucket']}/{key_suffix}"

            log.info(f"[resolve_load_params] {json.dumps(params, default=str)}")
            return params

        # ── TASK 2: Ingest fact data → Raw (S3/JSON) ─────────
        @task
        def ingest_to_raw(load_params: dict, **context):
            from operators.ingest.dhis2_ingest import run as dhis2_run
            return dhis2_run(config=config, env_config=env, load_params=load_params)

        # ── TASK 3: Short-circuit if no records (skips Glue/Redshift but still updates watermark) ──
        @task.short_circuit(ignore_downstream_trigger_rules=False)
        def has_data(ingest_result: dict) -> bool:
            count = ingest_result.get("record_count", 0)
            if count == 0:
                log.info("[has_data] Zero records — skipping Glue + Redshift; watermark will still advance")
                return False
            return True

        # ── TASK 4: Ensure crawler exists (create-once, NEW_FOLDERS_ONLY) ─────
        # The crawler definition really belongs in CDK, but until then we create
        # it on first run only and never mutate it again.
        @task
        def ensure_crawler(load_params: dict) -> str:
            import boto3
            glue_client = boto3.client("glue", region_name=env["aws_region"])
            crawler_name = f"{env['prefix']}-{pipeline_name}-curated-crawler"
            s3_target = f"s3://{env['curated_bucket']}/{load_params['country']}/{pipeline_name}/"
            try:
                glue_client.get_crawler(Name=crawler_name)
                log.info(f"[ensure_crawler] {crawler_name} already exists — leaving as-is")
            except glue_client.exceptions.EntityNotFoundException:
                log.info(f"[ensure_crawler] Creating {crawler_name}")
                glue_client.create_crawler(
                    Name=crawler_name,
                    Role=f"{env['prefix']}-glue-role",
                    DatabaseName=env["glue_database"],
                    Targets={"S3Targets": [{"Path": s3_target}]},
                    SchemaChangePolicy={
                        "UpdateBehavior": "UPDATE_IN_DATABASE",
                        "DeleteBehavior": "LOG",
                    },
                    # Only crawl new partitions — much cheaper than CRAWL_EVERYTHING
                    # once the bucket has historical data.
                    RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
                    TablePrefix=f"{pipeline_name}_",
                )
            return crawler_name

        # ── TASK 5: Load fact table to Redshift ──────────────
        @task
        def load_to_redshift(load_params: dict, **context):
            from operators.redshift_load import execute_load
            return execute_load(
                config=config,
                env_config=env,
                curated_s3_path=load_params["curated_s3_path"],
                load_params=load_params,
            )

        # ── TASK 6: Update watermark (runs even if Glue branch was skipped) ──
        @task(trigger_rule="none_failed")
        def update_watermark(load_params: dict, ingest_result: dict, load_result: dict = None, **context):
            from utils.cdc import set_watermark
            if load_params["mode"] != "incremental":
                return {"status": "skipped_non_incremental"}
            # Prefer Redshift max (canonical) but fall back to ingest's max for skip path.
            max_wm = (load_result or {}).get("max_watermark") or (ingest_result or {}).get("max_watermark")
            set_watermark(
                pipeline_name=pipeline_name,
                table_name=env["metadata_table"],
                execution_id=load_params["execution_id"],
                ingestion_time=load_params["ingestion_time"],
                max_watermark=max_wm,
            )
            return {"status": "watermark_updated", "max_watermark": max_wm}

        # ── TASK 7: Quality checks (only when fact load actually ran) ─────
        @task
        def run_quality_checks(load_params: dict, **context):
            from utils.quality import run_checks
            checks = config.get("quality_checks", [])
            if not checks:
                return {"status": "skipped", "reason": "no checks configured"}
            return run_checks(checks=checks, config=config, env_config=env)

        # ── Wire the DAG ─────────────────────────────────────
        params = resolve_load_params()
        ingested = ingest_to_raw(params)
        gate = has_data(ingested)

        # Glue jobs as deferrable operators — frees worker slot during multi-hour runs.
        custom_sql_processed = config.get("processed", {}).get("custom_sql", "")
        custom_sql_curated = config.get("curated", {}).get("custom_sql", "")
        dedup_key = config.get("processed", {}).get("deduplicate_key") or ""
        dedup_columns = config.get("processed", {}).get("deduplicate_columns") or ""
        source_format = config.get("raw", {}).get("format", "json")

        raw_to_processed_args = {
            "--source_path": "{{ ti.xcom_pull(task_ids='ingest_to_raw')['s3_path'] }}",
            "--target_bucket": env["processed_bucket"],
            "--pipeline_name": pipeline_name,
            "--execution_id": "{{ ti.xcom_pull(task_ids='resolve_load_params')['execution_id'] }}",
            "--country": "{{ ti.xcom_pull(task_ids='resolve_load_params')['country'] }}",
            "--ingestion_time": "{{ ti.xcom_pull(task_ids='resolve_load_params')['ingestion_time'] }}",
            "--custom_sql_path": f"s3://{env['assets_bucket']}/{custom_sql_processed}" if custom_sql_processed else "",
            "--load_mode": "{{ ti.xcom_pull(task_ids='resolve_load_params')['mode'] }}",
            "--metadata_table": env["metadata_table"],
            "--source_format": source_format,
        }
        if dedup_key:
            raw_to_processed_args["--dedup_key"] = dedup_key
        if dedup_columns:
            raw_to_processed_args["--dedup_columns"] = dedup_columns

        raw_to_processed_job = f"{env['prefix']}-{pipeline_name}-raw-to-processed"
        processed_to_curated_job = f"{env['prefix']}-{pipeline_name}-processed-to-curated"

        raw_to_processed = GlueJobOperator(
            task_id="raw_to_processed",
            job_name=raw_to_processed_job,
            region_name=env["aws_region"],
            script_args=raw_to_processed_args,
            wait_for_completion=True,
            # deferrable=True disabled: MWAA triggerer wasn't completing the
            # task even after Glue success. Revisit once triggerer logs / IAM
            # are confirmed working.
            deferrable=False,
            on_execute_callback=_log_glue_console_url(raw_to_processed_job, env["aws_region"]),
        )

        processed_to_curated = GlueJobOperator(
            task_id="processed_to_curated",
            job_name=processed_to_curated_job,
            region_name=env["aws_region"],
            script_args={
                "--source_path": "{{ ti.xcom_pull(task_ids='resolve_load_params')['processed_s3_path'] }}",
                "--target_bucket": env["curated_bucket"],
                "--pipeline_name": pipeline_name,
                "--execution_id": "{{ ti.xcom_pull(task_ids='resolve_load_params')['execution_id'] }}",
                "--country": "{{ ti.xcom_pull(task_ids='resolve_load_params')['country'] }}",
                "--ingestion_time": "{{ ti.xcom_pull(task_ids='resolve_load_params')['ingestion_time'] }}",
                "--custom_sql_path": f"s3://{env['assets_bucket']}/{custom_sql_curated}" if custom_sql_curated else "",
                "--metadata_table": env["metadata_table"],
            },
            wait_for_completion=True,
            # deferrable=True disabled: MWAA triggerer wasn't completing the
            # task even after Glue success. Revisit once triggerer logs / IAM
            # are confirmed working.
            deferrable=False,
            on_execute_callback=_log_glue_console_url(processed_to_curated_job, env["aws_region"]),
        )

        crawler_name_xcom = ensure_crawler(params)
        crawl_curated = GlueCrawlerOperator(
            task_id="crawl_curated",
            config={"Name": f"{env['prefix']}-{pipeline_name}-curated-crawler"},
            region_name=env["aws_region"],
            wait_for_completion=True,
            # deferrable=True disabled: MWAA triggerer wasn't completing the
            # task even after Glue success. Revisit once triggerer logs / IAM
            # are confirmed working.
            deferrable=False,
        )

        loaded = load_to_redshift(params)
        watermark = update_watermark(params, ingested, loaded)
        quality = run_quality_checks(params)

        # Wire the dependencies
        gate >> raw_to_processed >> processed_to_curated >> crawler_name_xcom >> crawl_curated >> loaded >> [watermark, quality]
        # update_watermark also runs on the skip-path (gate False) thanks to trigger_rule="none_failed"
        ingested >> watermark

    return dhis2_pipeline()


# ═══════════════════════════════════════════════════════════
# Auto-generate DHIS2 DAGs from YAML configs with dag_type: dhis2
# ═══════════════════════════════════════════════════════════
for _name, _cfg in load_all_pipeline_configs().items():
    if _cfg.get("pipeline", {}).get("dag_type") == "dhis2":
        globals()[f"etl_{_name}"] = create_dhis2_dag(_name, _cfg)
