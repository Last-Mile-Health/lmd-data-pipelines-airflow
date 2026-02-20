"""
OKR Dashboard ETL DAG — Transform (Redshift SQL) → Extract → RDS (PostgreSQL).

Flow:
    resolve_params
        └── refresh_redshift_views (CREATE OR REPLACE VIEW via Data API)
                └── for each view (parallel):
                        extract_from_redshift → load_to_rds

Auto-generated from YAML configs where dag_type == 'okr_redshift_to_rds'.
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


def create_okr_dag(pipeline_name: str, config: dict):
    """Create an OKR Redshift-to-RDS ETL DAG."""

    env = get_env_config()
    schedule_cfg = config.get("schedule", {})
    alerts_cfg = config.get("alerts", {})
    email_recipients = alerts_cfg.get("email_recipients", [])
    source_views = config.get("source", {}).get("views", [])
    target_cfg = config.get("target", {})
    transform_cfg = config.get("transforms", {})

    default_args = {
        "owner": config["pipeline"].get("owner", "data-team"),
        "retries": schedule_cfg.get("retries", 2),
        "retry_delay": timedelta(minutes=schedule_cfg.get("retry_delay_minutes", 5)),
        "execution_timeout": timedelta(minutes=schedule_cfg.get("timeout_minutes", 60)),
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
        tags=config["pipeline"].get("tags", []) + ["okr", "redshift-to-rds"],
        max_active_runs=1,
        doc_md=f"### OKR Dashboard Pipeline: `{pipeline_name}`\n\n{config['pipeline'].get('description', '')}",
    )
    def okr_pipeline():

        # ── TASK 1: Resolve parameters ──────────────────────
        _start_cb = None
        if email_recipients and alerts_cfg.get("email_on_start", False):
            _start_cb = lambda ctx: _on_start_callback(ctx, email_recipients, pipeline_name)

        @task(on_execute_callback=_start_cb)
        def resolve_run_params(**context):
            log.info("=" * 60)
            log.info(f"[resolve_run_params] START — pipeline={pipeline_name}")
            log.info(f"[resolve_run_params] env_config={json.dumps(env, indent=2)}")
            log.info(f"[resolve_run_params] redshift_secret_name={env.get('redshift_secret_name', 'NOT SET')}")
            log.info(f"[resolve_run_params] target.secret_name={target_cfg.get('secret_name', 'NOT SET')}")
            log.info(f"[resolve_run_params] target.writer_endpoint={target_cfg.get('writer_endpoint', 'NOT SET')}")

            execution_id = str(uuid.uuid4())
            ingestion_time = datetime.utcnow().isoformat()
            result = {
                "execution_id": execution_id,
                "ingestion_time": ingestion_time,
                "pipeline_name": pipeline_name,
            }
            log.info(f"[resolve_run_params] OUTPUT: {json.dumps(result, default=str)}")
            log.info("=" * 60)
            return result

        # ── TASK 2: Refresh Redshift views (transforms) ────
        @task
        def refresh_redshift_views(run_params: dict, **context):
            """Execute transform SQL files on Redshift (CREATE OR REPLACE VIEW, etc)."""
            log.info(f"[refresh_redshift_views] START — INPUT run_params={json.dumps(run_params, default=str)}")
            from operators.dbt_runner import run_transforms

            sql_dir = transform_cfg.get("sql_dir", "sql/okr_transforms")
            select = transform_cfg.get("select")

            result = run_transforms(
                config=config,
                env_config=env,
                sql_dir=sql_dir,
                select=select,
            )

            print(f"[OKR] Transforms completed: {result}")
            return result

        # ── TASK 3: Extract view and load to RDS ───────────
        @task
        def process_single_view(view_cfg: dict, run_params: dict, transform_result: dict, **context):
            log.info("=" * 60)
            log.info(f"[process_single_view] START — view={view_cfg}")
            log.info(f"[process_single_view] target_cfg={json.dumps(target_cfg, default=str)}")

            from operators.redshift_extract import extract_view
            from operators.rds_load import load_to_rds

            target_table = view_cfg["target_table"]
            view_label = f"{view_cfg.get('database', '')}.{view_cfg.get('schema', 'public')}.{view_cfg['view']}"

            # Extract from Redshift (views already refreshed by transform step)
            rows = extract_view(
                config=config,
                env_config=env,
                view_cfg=view_cfg,
            )

            if not rows:
                log.info(f"[process_single_view] No rows from {view_label} — skipping")
                return {"source_view": view_label, "status": "skipped", "rows": 0}

            log.info(f"[process_single_view] Extracted {len(rows)} rows from {view_label}")

            # Load to RDS
            result = load_to_rds(
                rows=rows,
                target_cfg=target_cfg,
                target_table=target_table,
                region=env["aws_region"],
            )

            output = {
                "source_view": view_label,
                "target_table": target_table,
                "status": result["status"],
                "rows_loaded": result.get("rows_loaded", 0),
            }
            log.info(f"[process_single_view] OUTPUT: {json.dumps(output, default=str)}")
            log.info("=" * 60)
            return output

        # ── Wire the DAG ─────────────────────────────────────
        run_params = resolve_run_params()
        transform_result = refresh_redshift_views(run_params)

        # Process all views in parallel (after transforms complete)
        for view_cfg in source_views:
            process_single_view(view_cfg, run_params, transform_result)

    return okr_pipeline()


# ═══════════════════════════════════════════════════════════
# Auto-generate OKR DAGs from YAML configs with dag_type: okr_redshift_to_rds
# ═══════════════════════════════════════════════════════════
for _name, _cfg in load_all_pipeline_configs().items():
    if _cfg.get("pipeline", {}).get("dag_type") == "okr_redshift_to_rds":
        globals()[f"etl_{_name}"] = create_okr_dag(_name, _cfg)
