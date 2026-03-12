"""
DAG Factory — Generates one Airflow DAG per pipeline YAML config.

Flow per pipeline:
    resolve_load_params → ingest_to_raw → raw_to_processed (Glue)
        → processed_to_curated (Glue) → crawl_curated (Glue Crawler)
        → load_to_redshift → update_watermark → run_quality_checks
"""
import os
import json
import logging
import uuid
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
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
    """Send email notification when DAG run starts (called on first task)."""
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
    """Send email notification when DAG run completes successfully."""
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


def create_etl_dag(pipeline_name: str, config: dict):
    """Create a full ETL DAG from a pipeline YAML config."""

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

    # Email notifications
    dag_callbacks = {}
    if email_recipients:
        if alerts_cfg.get("email_on_failure", True):
            default_args["on_failure_callback"] = lambda ctx: _on_failure_callback(ctx, email_recipients, pipeline_name)
        if alerts_cfg.get("email_on_success", True):
            dag_callbacks["on_success_callback"] = lambda ctx: _on_success_callback(ctx, email_recipients, pipeline_name)
        default_args["email"] = email_recipients
        default_args["email_on_failure"] = alerts_cfg.get("email_on_failure", True)
        default_args["email_on_retry"] = alerts_cfg.get("email_on_retry", False)

    @dag(
        dag_id=f"etl_{pipeline_name}",
        default_args=default_args,
        schedule=schedule_cfg.get("cron"),
        start_date=datetime(2024, 1, 1),
        catchup=schedule_cfg.get("catchup", False),
        tags=config["pipeline"].get("tags", []),
        max_active_runs=1,
        doc_md=f"""### ETL Pipeline: `{pipeline_name}`\n\n{config['pipeline'].get('description', '')}""",
        **dag_callbacks,
    )
    def etl_pipeline():

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
            from utils.cdc import get_watermark, compute_boundaries

            # Runtime overrides via dag_run.conf (defaults from YAML)
            conf = context["dag_run"].conf or {}
            mode = conf.get("mode", config["ingestion"]["mode"])  # full | incremental
            log.info(f"[resolve_load_params] mode={mode} (default={config['ingestion']['mode']}, override={conf.get('mode', 'none')})")

            execution_id = str(uuid.uuid4())
            ingestion_time = datetime.utcnow().isoformat()

            params = {
                "execution_id": execution_id,
                "ingestion_time": ingestion_time,
                "mode": mode,
                "pipeline_name": pipeline_name,
                "country": conf.get("country", config["source"]["config"].get("default_country", "global")),
            }

            if mode == "incremental":
                watermark = get_watermark(
                    pipeline_name=pipeline_name,
                    table_name=env["metadata_table"],
                )
                boundaries = compute_boundaries(
                    watermark=watermark,
                    incremental_key=config["ingestion"].get("incremental_key"),
                )
                params.update(boundaries)
                print(f"[CDC] Watermark from DynamoDB: {watermark}")
                print(f"[CDC] Boundaries: start_after={boundaries.get('start_after')}")
            else:
                params["watermark"] = None
                print(f"[CDC] Mode=full — no watermark filter")

            log.info(f"[resolve_load_params] OUTPUT: {json.dumps(params, default=str)}")
            log.info("=" * 60)
            return params

        # ── TASK 2: Ingest from source → Raw (S3/JSON) ────────
        @task
        def ingest_to_raw(load_params: dict, **context):
            """
            Route to the correct ingestor based on source.type.
            Writes raw data (JSON) to Raw layer in S3.
            Returns S3 path of ingested data.
            """
            log.info("=" * 60)
            log.info(f"[ingest_to_raw] START — pipeline={pipeline_name}")
            log.info(f"[ingest_to_raw] INPUT load_params={json.dumps(load_params, default=str)}")
            log.info(f"[ingest_to_raw] source_type={config['source']['type']}")
            log.info(f"[ingest_to_raw] source.secret_name={config.get('source', {}).get('secret_name', 'NOT SET')}")

            from operators.ingest.csv_ingest import run as csv_run
            from operators.ingest.dhis2_ingest import run as dhis2_run
            from operators.ingest.api_ingest import run as api_run
            from operators.ingest.kobo_ingest import run as kobo_run

            ingestors = {
                "csv": csv_run,
                "dhis2": dhis2_run,
                "api": api_run,
                "kobo_api": kobo_run,
            }

            source_type = config["source"]["type"]
            ingestor = ingestors.get(source_type)
            if not ingestor:
                raise ValueError(f"Unknown source type: {source_type}")

            result = ingestor(
                config=config,
                env_config=env,
                load_params=load_params,
            )

            log.info(f"[ingest_to_raw] OUTPUT: {json.dumps(result, default=str)}")
            log.info("=" * 60)

            # Short-circuit: no new data to process
            if result.get("record_count", 0) == 0:
                from airflow.exceptions import AirflowSkipException
                raise AirflowSkipException("No new records to process — skipping downstream tasks")

            return result

        # ── TASK 3: Raw → Processed (Glue Spark) ──────────────
        @task
        def trigger_raw_to_processed(ingest_result: dict, load_params: dict, **context):
            """Start Glue job for Raw→Processed transformation (fully dynamic, no custom SQL)."""
            log.info(f"[raw_to_processed] START — INPUT ingest_result={json.dumps(ingest_result, default=str)}")
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

            # Poll until complete
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

            # Return processed path
            now = datetime.fromisoformat(load_params["ingestion_time"])
            processed_key = (
                f"{load_params['country']}/{pipeline_name}/"
                f"{now.strftime('%Y/%m/%d')}/"
                f"{load_params['execution_id']}/"
            )
            result = {
                "s3_path": f"s3://{env['processed_bucket']}/{processed_key}",
                "processed_key": processed_key,
            }
            log.info(f"[raw_to_processed] OUTPUT: {json.dumps(result, default=str)}")
            return result

        # ── TASK 4: Processed → Curated (Glue Spark) ──────────
        @task
        def trigger_processed_to_curated(processed_result: dict, load_params: dict, **context):
            """Start Glue job for Processed→Curated transformation (business logic via custom SQL)."""
            log.info(f"[processed_to_curated] START — INPUT processed_result={json.dumps(processed_result, default=str)}")
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
            result = {
                "s3_path": f"s3://{env['curated_bucket']}/{curated_key}",
                "curated_key": curated_key,
            }
            log.info(f"[processed_to_curated] OUTPUT: {json.dumps(result, default=str)}")
            return result

        # ── TASK 5a: Crawl Curated S3 (Glue Crawler) ────────────
        @task
        def crawl_curated(curated_result: dict, load_params: dict, **context):
            """Run Glue Crawler on curated S3 path to register/update schema in Glue Data Catalog."""
            import boto3
            import time as _time

            glue_client = boto3.client("glue", region_name=env["aws_region"])
            crawler_name = f"{env['prefix']}-{pipeline_name}-curated-crawler"

            # Point crawler at the curated S3 path for this pipeline/country
            s3_target = (
                f"s3://{env['curated_bucket']}/"
                f"{load_params['country']}/{pipeline_name}/"
            )

            # Create or update the crawler
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

            # Start the crawler
            glue_client.start_crawler(Name=crawler_name)

            # Poll until complete
            while True:
                resp = glue_client.get_crawler(Name=crawler_name)
                state = resp["Crawler"]["State"]
                if state == "READY":
                    break
                _time.sleep(10)

            return curated_result

        # ── TASK 5b: Curated → Redshift ─────────────────────────
        @task
        def load_to_redshift(curated_result: dict, load_params: dict, **context):
            """Load curated data from S3 into Redshift."""
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

        # ── TASK 6: Update watermark ───────────────────────────
        @task
        def update_watermark(load_result: dict, load_params: dict, **context):
            """Persist new high watermark after successful load."""
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

        # ── TASK 7: Data quality checks ────────────────────────
        @task
        def run_quality_checks(load_params: dict, **context):
            """Run configured quality checks against Redshift."""
            from utils.quality import run_checks

            checks = config.get("quality_checks", [])
            if not checks:
                return {"status": "skipped", "reason": "no checks configured"}

            return run_checks(
                checks=checks,
                config=config,
                env_config=env,
            )

        # ── Wire the DAG ──────────────────────────────────────
        params = resolve_load_params()
        ingested = ingest_to_raw(params)
        processed = trigger_raw_to_processed(ingested, params)
        curated = trigger_processed_to_curated(processed, params)
        crawled = crawl_curated(curated, params)
        loaded = load_to_redshift(crawled, params)
        updated = update_watermark(loaded, params)
        updated >> run_quality_checks(params)

    return etl_pipeline()


# ═══════════════════════════════════════════════════════════
# Auto-generate DAGs from all YAML configs
# ═══════════════════════════════════════════════════════════
_all_configs = load_all_pipeline_configs()
log.info(f"[DAG Factory] Loaded {len(_all_configs)} pipeline configs: {list(_all_configs.keys())}")
for _name, _cfg in _all_configs.items():
    log.info(f"[DAG Factory] {_name}: secret_name={_cfg.get('source', {}).get('secret_name', 'N/A')}, dag_type={_cfg.get('pipeline', {}).get('dag_type', 'generic')}")
    # Skip pipelines handled by dedicated DAGs (e.g. dag_type: dhis2)
    if _cfg.get("pipeline", {}).get("dag_type"):
        continue
    globals()[f"etl_{_name}"] = create_etl_dag(_name, _cfg)
