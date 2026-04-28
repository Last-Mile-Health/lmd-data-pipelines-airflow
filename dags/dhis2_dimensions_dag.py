"""
DHIS2 Dimensions DAG — refresh dimension tables on their own schedule.

Dimensions (dataElements, organisationUnits, categoryOptionCombos, …) hardly
change, so refreshing them on every fact-load run wastes API calls + Redshift
COPY work. This DAG runs on the cadence configured in
`dimensions_refresh.cron` (default: monthly) AND each task short-circuits if a
per-dim DynamoDB watermark is younger than `min_refresh_days`.

Net effect: even if the DAG is manually triggered every day, dims only re-pull
from DHIS2 once per `min_refresh_days` window.
"""
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from utils.config_loader import load_all_pipeline_configs, get_env_config, build_flow_tags

log = logging.getLogger(__name__)


def create_dhis2_dimensions_dag(pipeline_name: str, config: dict):
    env = get_env_config()
    refresh_cfg = config.get("dimensions_refresh", {}) or {}
    dimensions_cfg = config.get("dimensions", []) or []
    cron = refresh_cfg.get("cron", "0 6 1 * *")
    min_refresh_days = int(refresh_cfg.get("min_refresh_days", 25))
    default_country = config.get("source", {}).get("config", {}).get("default_country", "global")

    default_args = {
        "owner": config["pipeline"].get("owner", "data-team"),
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=60),
    }

    @dag(
        dag_id=f"etl_{pipeline_name}_dimensions",
        default_args=default_args,
        description=f"Dimension refresh for {pipeline_name} (watermark-gated)",
        schedule=cron,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        # Stage tags MUST be sourced from build_flow_tags — see AGENTS.local.md.
        # Force dhis2_dimensions tag shape regardless of the YAML's pipeline.dag_type
        # (which is "dhis2" so the fact DAG factory picks it up too).
        tags=(
            config["pipeline"].get("tags", [])
            + ["dhis2", "dimensions"]
            + build_flow_tags({**config, "pipeline": {**config["pipeline"], "dag_type": "dhis2_dimensions"}})
        ),
        max_active_runs=1,
        doc_md=(
            f"Refreshes DHIS2 dimensions for `{pipeline_name}` on `{cron}`.\n\n"
            f"Each dim is skipped if refreshed within the last "
            f"`{min_refresh_days}` days (DynamoDB-tracked)."
        ),
    )
    def dimensions_pipeline():

        @task
        def refresh_one_dimension(dim_cfg: dict, **context):
            import uuid
            import boto3
            from utils.cdc import dimension_is_fresh, set_dimension_watermark
            from operators.ingest.dhis2_metadata_ingest import run_dimension
            from operators.redshift_load import load_dimension_json, _get_redshift_secret

            dim_name = dim_cfg["name"]

            # Watermark gate — skip if refreshed recently
            if dimension_is_fresh(pipeline_name, dim_name, env["metadata_table"], min_refresh_days):
                log.info(f"[DIM] {dim_name}: fresh (< {min_refresh_days}d) — skipping")
                return {"dim_name": dim_name, "status": "skipped_fresh"}

            execution_id = str(uuid.uuid4())
            ingestion_time = datetime.utcnow().isoformat()
            load_params = {
                "execution_id": execution_id,
                "ingestion_time": ingestion_time,
                "pipeline_name": pipeline_name,
                "country": default_country,
            }

            result = run_dimension(
                dimension_cfg=dim_cfg,
                source_cfg=config["source"]["config"],
                secret_name=config["source"].get("secret_name"),
                env_config=env,
                load_params=load_params,
            )

            if result["record_count"] == 0:
                log.info(f"[DIM] {dim_name}: zero records — not loading")
                return {"dim_name": dim_name, "status": "zero_records"}

            secret_arn, secret_values = _get_redshift_secret(
                env["redshift_secret_name"], env["aws_region"]
            )
            client = boto3.client("redshift-data", region_name=env["aws_region"])
            redshift_cfg = config.get("redshift", {})
            database = redshift_cfg.get("database") or env["redshift_database"]

            load_dimension_json(
                client=client,
                workgroup=secret_values["workgroupName"],
                database=database,
                secret_arn=secret_arn,
                schema=dim_cfg.get("schema", "public"),
                table=dim_cfg["table"],
                s3_path=result["s3_path"],
                iam_role=env["redshift_iam_role_arn"],
                columns=dim_cfg["columns"],
            )

            set_dimension_watermark(
                pipeline_name=pipeline_name,
                dim_name=dim_name,
                table_name=env["metadata_table"],
                execution_id=execution_id,
                record_count=result["record_count"],
            )
            return {"dim_name": dim_name, "status": "loaded", "record_count": result["record_count"]}

        # Fan out: one task per configured dimension. Failures don't block siblings.
        for dim_cfg in dimensions_cfg:
            refresh_one_dimension.override(task_id=f"refresh_{dim_cfg['name']}")(dim_cfg)

    return dimensions_pipeline()


# Auto-generate dimensions DAGs alongside fact DAGs
for _name, _cfg in load_all_pipeline_configs().items():
    if _cfg.get("pipeline", {}).get("dag_type") == "dhis2" and _cfg.get("dimensions"):
        globals()[f"etl_{_name}_dimensions"] = create_dhis2_dimensions_dag(_name, _cfg)
