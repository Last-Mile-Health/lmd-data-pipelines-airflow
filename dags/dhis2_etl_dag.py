"""
Dedicated DHIS2 ETL DAG — Star-schema pipeline with dimension loading.

Flow:
    resolve_load_params
        ├── load_dimensions (parallel per dimension)
        └── ingest_to_raw → raw_to_processed (Glue)
                → processed_to_curated (Glue) → crawl_curated (Glue Crawler)
                    → load_to_redshift  ← waits for dims + crawl
                        → post_sql (BI view) + ANALYZE
                            → update_watermark → run_quality_checks

Only instantiated if config/pipelines/lib_dhis2_pipeline.yml exists
and has pipeline.dag_type == 'dhis2'.
"""
import os
import json
import logging
import uuid
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.email import send_email

from utils.config_loader import load_all_pipeline_configs, get_env_config

log = logging.getLogger(__name__)


def _on_failure_callback(context, recipients, pipeline_name):
    """Send email notification on task failure."""
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
    """Send email notification when DAG run starts."""
    send_email(
        to=recipients,
        subject=f"[STARTED] Pipeline: {pipeline_name}",
        html_content=f"""
        <h3>Pipeline Started</h3>
        <p><b>Pipeline:</b> {pipeline_name}</p>
        <p><b>Execution Date:</b> {context.get('execution_date')}</p>
        <p><b>Run ID:</b> {context.get('run_id')}</p>
        """,
    )


def create_dhis2_dag(pipeline_name: str, config: dict):
    """Create the dedicated DHIS2 ETL DAG with dimension loading."""

    env = get_env_config()
    schedule_cfg = config.get("schedule", {})
    alerts_cfg = config.get("alerts", {})
    email_recipients = alerts_cfg.get("email_recipients", [])
    dimensions_cfg = config.get("dimensions", [])

    default_args = {
        "owner": config["pipeline"].get("owner", "data-team"),
        "retries": schedule_cfg.get("retries", 2),
        "retry_delay": timedelta(minutes=schedule_cfg.get("retry_delay_minutes", 5)),
        "execution_timeout": timedelta(minutes=schedule_cfg.get("timeout_minutes", 120)),
    }

    if email_recipients:
        if alerts_cfg.get("email_on_failure", True):
            default_args["on_failure_callback"] = lambda ctx: _on_failure_callback(ctx, email_recipients, pipeline_name)
        default_args["email"] = email_recipients
        default_args["email_on_failure"] = alerts_cfg.get("email_on_failure", True)
        default_args["email_on_retry"] = alerts_cfg.get("email_on_retry", False)

    @dag(
        dag_id=f"etl_{pipeline_name}",
        default_args=default_args,
        schedule=schedule_cfg.get("cron"),
        start_date=datetime(2024, 1, 1),
        catchup=schedule_cfg.get("catchup", False),
        tags=config["pipeline"].get("tags", []) + ["star-schema"],
        max_active_runs=1,
        doc_md=f"### DHIS2 Star-Schema Pipeline: `{pipeline_name}`\n\n{config['pipeline'].get('description', '')}",
    )
    def dhis2_pipeline():

        # ── TASK 1: Determine load boundaries ──────────────────
        _start_cb = None
        if email_recipients and alerts_cfg.get("email_on_start", False):
            _start_cb = lambda ctx: _on_start_callback(ctx, email_recipients, pipeline_name)

        @task(on_execute_callback=_start_cb)
        def resolve_load_params(**context):
            log.info("=" * 60)
            log.info(f"[resolve_load_params] START — pipeline={pipeline_name}")
            log.info(f"[resolve_load_params] env_config={json.dumps(env, indent=2)}")
            log.info(f"[resolve_load_params] source.secret_name={config.get('source', {}).get('secret_name', 'NOT SET')}")
            log.info(f"[resolve_load_params] redshift_secret_name={env.get('redshift_secret_name', 'NOT SET')}")
            log.info(f"[resolve_load_params] redshift_iam_role_arn={env.get('redshift_iam_role_arn', 'NOT SET')}")

            from dateutil.relativedelta import relativedelta
            from utils.cdc import get_watermark, compute_boundaries

            conf = context["dag_run"].conf or {}
            ingestion_cfg = config["ingestion"]

            # CDC strategy: runtime override > YAML cdc_strategy > legacy mode fallback
            cdc_strategy = conf.get(
                "cdc_strategy_override",
                ingestion_cfg.get("cdc_strategy", ingestion_cfg.get("mode", "full")),
            )

            execution_id = str(uuid.uuid4())
            ingestion_time = datetime.utcnow().isoformat()

            params = {
                "execution_id": execution_id,
                "ingestion_time": ingestion_time,
                "cdc_strategy": cdc_strategy,
                "pipeline_name": pipeline_name,
                "country": conf.get("country", config["source"]["config"].get("default_country", "global")),
            }

            # Pass runtime overrides for source params (period, dataSet, orgUnit, etc.)
            # period can be a single string "202601" or a list ["202601", "202602"]
            source_params = conf.get("source_params", {})
            if source_params:
                params["source_params"] = source_params
                log.info(f"[resolve_load_params] Runtime source_params overrides: {source_params}")

                # If user explicitly provided period(s), normalize to a list
                if "period" in source_params:
                    user_periods = source_params["period"]
                    if isinstance(user_periods, str):
                        user_periods = [p.strip() for p in user_periods.split(",") if p.strip()]
                    source_params["period"] = user_periods
                    params["period_list"] = user_periods
                    log.info(f"[resolve_load_params] User-specified periods: {user_periods}")

            if cdc_strategy == "incremental":
                params["mode"] = "incremental"
                watermark = get_watermark(
                    pipeline_name=pipeline_name,
                    table_name=env["metadata_table"],
                )
                boundaries = compute_boundaries(
                    watermark=watermark,
                    incremental_key=ingestion_cfg.get("incremental_key"),
                )
                params.update(boundaries)
                print(f"[CDC] Strategy=incremental | Watermark from DynamoDB: {watermark}")

            elif cdc_strategy == "rolling_window":
                params["mode"] = "rolling_window"
                params["watermark"] = None
                window_months = ingestion_cfg.get("rolling_window_months", 3)
                now = datetime.utcnow()
                period_list = []
                for i in range(window_months):
                    dt = now - relativedelta(months=i)
                    period_list.append(dt.strftime("%Y%m"))
                params["period_list"] = period_list
                print(f"[CDC] Strategy=rolling_window | Periods: {period_list}")

            else:
                # full (default)
                params["mode"] = "full"
                params["watermark"] = None
                print(f"[CDC] Strategy=full — static params, no watermark filter")

            log.info(f"[resolve_load_params] OUTPUT: {json.dumps(params, default=str)}")
            log.info("=" * 60)
            return params

        # ── TASK 2a: Load dimensions (parallel) ───────────────
        @task
        def load_single_dimension(dim_cfg: dict, load_params: dict, **context):
            """Fetch one DHIS2 metadata endpoint and load into Redshift dimension table."""
            log.info("=" * 60)
            log.info(f"[load_single_dimension] START — dim={dim_cfg.get('name')}")
            log.info(f"[load_single_dimension] source.secret_name={config.get('source', {}).get('secret_name', 'NOT SET')}")
            log.info(f"[load_single_dimension] redshift_secret_name={env.get('redshift_secret_name', 'NOT SET')}")

            import boto3
            from operators.ingest.dhis2_metadata_ingest import run_dimension
            from operators.redshift_load import (
                load_dimension_json,
                _get_redshift_secret,
            )

            # 1. Ingest metadata to S3
            result = run_dimension(
                dimension_cfg=dim_cfg,
                source_cfg=config["source"]["config"],
                secret_name=config["source"].get("secret_name"),
                env_config=env,
                load_params=load_params,
            )

            if result["record_count"] == 0:
                print(f"[DIM] {dim_cfg['name']}: no records — skipping Redshift load")
                return result

            # 2. Load into Redshift
            redshift_cfg = config["redshift"]
            database = redshift_cfg.get("database") or env["redshift_database"]
            secret_arn, secret_values = _get_redshift_secret(
                env["redshift_secret_name"], env["aws_region"]
            )
            workgroup = secret_values["workgroupName"]
            client = boto3.client("redshift-data", region_name=env["aws_region"])

            load_dimension_json(
                client=client,
                workgroup=workgroup,
                database=database,
                secret_arn=secret_arn,
                schema=dim_cfg.get("schema", "public"),
                table=dim_cfg["table"],
                s3_path=result["s3_path"],
                iam_role=env["redshift_iam_role_arn"],
                columns=dim_cfg["columns"],
            )

            log.info(f"[load_single_dimension] OUTPUT: {json.dumps(result, default=str)}")
            log.info("=" * 60)
            return result

        # ── TASK 2b: Ingest fact data → Raw (S3/JSON) ─────────
        @task
        def ingest_to_raw(load_params: dict, **context):
            log.info("=" * 60)
            log.info(f"[ingest_to_raw] START — pipeline={pipeline_name}")
            log.info(f"[ingest_to_raw] INPUT load_params={json.dumps(load_params, default=str)}")
            log.info(f"[ingest_to_raw] source.secret_name={config.get('source', {}).get('secret_name', 'NOT SET')}")
            from operators.ingest.dhis2_ingest import run as dhis2_run

            result = dhis2_run(
                config=config,
                env_config=env,
                load_params=load_params,
            )

            log.info(f"[ingest_to_raw] OUTPUT: {json.dumps(result, default=str)}")
            log.info("=" * 60)

            if result.get("record_count", 0) == 0:
                from airflow.exceptions import AirflowSkipException
                raise AirflowSkipException("No new records to process — skipping downstream tasks")

            return result

        # ── TASK 3: Raw → Processed (Glue Spark) ──────────────
        @task
        def trigger_raw_to_processed(ingest_result: dict, load_params: dict, **context):
            import boto3

            glue_client = boto3.client("glue", region_name=env["aws_region"])
            job_name = f"{env['prefix']}-{pipeline_name}-raw-to-processed"

            custom_sql_path = ""
            sql_file = config.get("processed", {}).get("custom_sql")
            if sql_file:
                custom_sql_path = f"s3://{env['assets_bucket']}/{sql_file}"

            glue_args = {
                "--source_path": ingest_result["s3_path"],
                "--target_bucket": env["processed_bucket"],
                "--pipeline_name": pipeline_name,
                "--execution_id": load_params["execution_id"],
                "--country": load_params["country"],
                "--ingestion_time": load_params["ingestion_time"],
                "--custom_sql_path": custom_sql_path,
                "--load_mode": load_params["mode"],
                "--metadata_table": env["metadata_table"],
                "--source_format": config.get("raw", {}).get("format", "json"),
            }

            dedup_key = config.get("processed", {}).get("deduplicate_key")
            if dedup_key:
                glue_args["--dedup_key"] = dedup_key

            response = glue_client.start_job_run(
                JobName=job_name,
                Arguments=glue_args,
            )
            job_run_id = response["JobRunId"]

            import time
            while True:
                status = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
                state = status["JobRun"]["JobRunState"]
                if state in ("SUCCEEDED",):
                    break
                if state in ("FAILED", "TIMEOUT", "STOPPED", "ERROR"):
                    error = status["JobRun"].get("ErrorMessage", "Unknown error")
                    raise RuntimeError(f"Glue job {job_name} failed: {error}")
                time.sleep(30)

            now = datetime.fromisoformat(load_params["ingestion_time"])
            processed_key = (
                f"{load_params['country']}/{pipeline_name}/"
                f"{now.strftime('%Y/%m/%d')}/"
                f"{load_params['execution_id']}/"
            )
            return {
                "s3_path": f"s3://{env['processed_bucket']}/{processed_key}",
                "processed_key": processed_key,
            }

        # ── TASK 4: Processed → Curated (Glue Spark) ──────────
        @task
        def trigger_processed_to_curated(processed_result: dict, load_params: dict, **context):
            import boto3

            glue_client = boto3.client("glue", region_name=env["aws_region"])
            job_name = f"{env['prefix']}-{pipeline_name}-processed-to-curated"

            custom_sql_path = ""
            sql_file = config.get("curated", {}).get("custom_sql")
            if sql_file:
                custom_sql_path = f"s3://{env['assets_bucket']}/{sql_file}"

            response = glue_client.start_job_run(
                JobName=job_name,
                Arguments={
                    "--source_path": processed_result["s3_path"],
                    "--target_bucket": env["curated_bucket"],
                    "--pipeline_name": pipeline_name,
                    "--execution_id": load_params["execution_id"],
                    "--country": load_params["country"],
                    "--ingestion_time": load_params["ingestion_time"],
                    "--custom_sql_path": custom_sql_path,
                    "--metadata_table": env["metadata_table"],
                },
            )
            job_run_id = response["JobRunId"]

            import time
            while True:
                status = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
                state = status["JobRun"]["JobRunState"]
                if state in ("SUCCEEDED",):
                    break
                if state in ("FAILED", "TIMEOUT", "STOPPED", "ERROR"):
                    error = status["JobRun"].get("ErrorMessage", "Unknown error")
                    raise RuntimeError(f"Glue job {job_name} failed: {error}")
                time.sleep(30)

            now = datetime.fromisoformat(load_params["ingestion_time"])
            curated_key = (
                f"{load_params['country']}/{pipeline_name}/"
                f"{now.strftime('%Y/%m/%d')}/"
                f"{load_params['execution_id']}/"
            )
            return {
                "s3_path": f"s3://{env['curated_bucket']}/{curated_key}",
                "curated_key": curated_key,
            }

        # ── TASK 5a: Crawl Curated S3 (Glue Crawler) ──────────
        @task
        def crawl_curated(curated_result: dict, load_params: dict, **context):
            import boto3
            import time as _time

            glue_client = boto3.client("glue", region_name=env["aws_region"])
            crawler_name = f"{env['prefix']}-{pipeline_name}-curated-crawler"

            s3_target = (
                f"s3://{env['curated_bucket']}/"
                f"{load_params['country']}/{pipeline_name}/"
            )

            crawler_config = {
                "Name": crawler_name,
                "Role": f"{env['prefix']}-glue-role",
                "DatabaseName": env["glue_database"],
                "Targets": {"S3Targets": [{"Path": s3_target}]},
                "SchemaChangePolicy": {
                    "UpdateBehavior": "UPDATE_IN_DATABASE",
                    "DeleteBehavior": "LOG",
                },
                "RecrawlPolicy": {"RecrawlBehavior": "CRAWL_EVERYTHING"},
                "TablePrefix": f"{pipeline_name}_",
            }

            try:
                glue_client.get_crawler(Name=crawler_name)
                glue_client.update_crawler(**crawler_config)
            except glue_client.exceptions.EntityNotFoundException:
                glue_client.create_crawler(**crawler_config)

            glue_client.start_crawler(Name=crawler_name)

            while True:
                resp = glue_client.get_crawler(Name=crawler_name)
                state = resp["Crawler"]["State"]
                if state == "READY":
                    break
                _time.sleep(10)

            return curated_result

        # ── TASK 5b: Load fact table to Redshift ───────────────
        # Waits for both crawl_curated AND all load_dimensions (via >> ordering)
        @task
        def load_to_redshift(curated_result: dict, load_params: dict, **context):
            """Load curated fact data into Redshift. Runs after dimensions are loaded."""
            log.info(f"[load_to_redshift] START — INPUT curated_s3_path={curated_result['s3_path']}")
            log.info(f"[load_to_redshift] redshift_secret={env.get('redshift_secret_name')}, iam_role={env.get('redshift_iam_role_arn')}")
            from operators.redshift_load import execute_load

            result = execute_load(
                config=config,
                env_config=env,
                curated_s3_path=curated_result["s3_path"],
                load_params=load_params,
            )
            log.info(f"[load_to_redshift] OUTPUT: {json.dumps(result, default=str)}")
            return result

        # ── TASK 6: Update watermark ──────────────────────────
        @task
        def update_watermark(load_result: dict, load_params: dict, **context):
            from utils.cdc import set_watermark

            if load_params["mode"] == "incremental":
                set_watermark(
                    pipeline_name=pipeline_name,
                    table_name=env["metadata_table"],
                    execution_id=load_params["execution_id"],
                    ingestion_time=load_params["ingestion_time"],
                    max_watermark=load_result.get("max_watermark"),
                )
            return {"status": "watermark_updated"}

        # ── TASK 7: Data quality checks ───────────────────────
        @task
        def run_quality_checks(load_params: dict, **context):
            from utils.quality import run_checks

            checks = config.get("quality_checks", [])
            if not checks:
                return {"status": "skipped", "reason": "no checks configured"}

            return run_checks(
                checks=checks,
                config=config,
                env_config=env,
            )

        # ── Wire the DAG ─────────────────────────────────────
        params = resolve_load_params()

        # Dimension loading (parallel) — runs alongside fact ingest
        dim_results = [
            load_single_dimension(dim_cfg, params)
            for dim_cfg in dimensions_cfg
        ]

        # Fact pipeline: ingest → process → curate → crawl
        ingested = ingest_to_raw(params)
        processed = trigger_raw_to_processed(ingested, params)
        curated = trigger_processed_to_curated(processed, params)
        crawled = crawl_curated(curated, params)

        # Fact load waits for both crawled data AND all dimensions
        loaded = load_to_redshift(crawled, params)
        # Set dimension tasks as upstream ordering dependencies
        for dim_result in dim_results:
            dim_result >> loaded
        updated = update_watermark(loaded, params)
        updated >> run_quality_checks(params)

    return dhis2_pipeline()


# ═══════════════════════════════════════════════════════════
# Auto-generate DHIS2 DAGs from YAML configs with dag_type: dhis2
# ═══════════════════════════════════════════════════════════
for _name, _cfg in load_all_pipeline_configs().items():
    if _cfg.get("pipeline", {}).get("dag_type") == "dhis2":
        globals()[f"etl_{_name}"] = create_dhis2_dag(_name, _cfg)
