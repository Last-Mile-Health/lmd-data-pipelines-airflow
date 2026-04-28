"""
Pipeline configuration loader.

Reads YAML configs from config/pipelines/ and provides them to the DAG factory.
Each YAML file defines one pipeline (source, layers, schedule, load mode, etc).
"""
import os
import re
import glob
import yaml
from functools import lru_cache
from typing import Dict, Any


# In MWAA, config is at /usr/local/airflow/dags/config/pipelines/
# (CDK uploads config/ into the dags/ prefix in S3)
# Locally, config is at ../../config/pipelines from dags/utils/
_MWAA_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config", "pipelines")
_LOCAL_CONFIG = os.path.join(os.path.dirname(__file__), "..", "..", "config", "pipelines")
CONFIG_DIR = _MWAA_CONFIG if os.path.isdir(_MWAA_CONFIG) else _LOCAL_CONFIG

_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


def _get_env(var_name: str, default: str = "") -> str:
    """Get env var, checking both direct name and MWAA env_var convention.

    MWAA config `env_var.foo_bar` becomes env var `AIRFLOW__ENV_VAR__FOO_BAR`.
    We check: VAR_NAME → AIRFLOW__ENV_VAR__VAR_NAME (uppercased).
    """
    upper = var_name.upper()
    return (
        os.environ.get(var_name)
        or os.environ.get(upper)
        or os.environ.get(f"AIRFLOW__ENV_VAR__{upper}")
        or default
    )


def _resolve_env_vars(obj):
    """Recursively resolve ${VAR_NAME} placeholders in config values."""
    if isinstance(obj, str):
        return _ENV_VAR_PATTERN.sub(
            lambda m: _get_env(m.group(1), m.group(0)), obj
        )
    if isinstance(obj, dict):
        return {k: _resolve_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env_vars(item) for item in obj]
    return obj


def load_pipeline_config(pipeline_name: str) -> Dict[str, Any]:
    """Load a single pipeline config by name."""
    filepath = os.path.join(CONFIG_DIR, f"{pipeline_name}.yml")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Pipeline config not found: {filepath}")
    with open(filepath) as f:
        return _resolve_env_vars(yaml.safe_load(f))


def _configs_signature() -> tuple:
    """Cheap fingerprint of the config dir so the cache invalidates on YAML edits."""
    pattern = os.path.join(CONFIG_DIR, "*.yml")
    return tuple(sorted((p, os.path.getmtime(p)) for p in glob.glob(pattern)))


@lru_cache(maxsize=8)
def _load_all_cached(signature: tuple) -> Dict[str, Dict[str, Any]]:
    configs = {}
    for filepath, _mtime in signature:
        with open(filepath) as f:
            config = _resolve_env_vars(yaml.safe_load(f))
            if config and "pipeline" in config:
                name = config["pipeline"]["name"]
                configs[name] = config
    return configs


def load_all_pipeline_configs() -> Dict[str, Dict[str, Any]]:
    """
    Load all pipeline configs from config/pipelines/*.yml.

    Cached on (filepath, mtime) tuple so repeated scheduler heartbeats don't
    re-parse YAML when nothing changed.
    """
    return _load_all_cached(_configs_signature())


_SOURCE_LABELS = {
    "kobo_api": "Kobo API",
    "dhis2":    "DHIS2 API",
    "csv":      "CSV (S3)",
    "api":      "REST API",
    "redshift": "Redshift",
}


def build_flow_tags(config: dict) -> list:
    """
    Build ordered pipeline-stage tags for the Airflow REST API.

    Each tag has the format: stage:<n>|<Label>|<detail>
    Clients can filter tags starting with "stage:", split on "|", and render
    each as a labelled box in order.

    Generic / DHIS2 pipeline stages:
        stage:1|Source|<type + form/endpoint>
        stage:2|S3 Raw|<format>
        stage:3|S3 Processed|Parquet
        stage:4|S3 Curated|Parquet
        stage:5|Destination|Redshift <db>.<schema>.<table>

    OKR (Redshift → RDS) stages:
        stage:1|Source|Redshift <db>.<schema>.<view>
        stage:2|Destination|RDS <dbname>.<schema>.<table>
    """
    dag_type = config.get("pipeline", {}).get("dag_type", "")

    if dag_type == "dhis2_dimensions":
        rs = config.get("redshift", {}) or {}
        rs_db = rs.get("database", "")
        rs_schema = rs.get("schema", "public")
        n_dims = sum(1 for d in config.get("dimensions", []) if d.get("table"))
        dest_detail = f"{rs_db}.{rs_schema} ({n_dims} dim tables)" if n_dims else rs_db
        # Airflow tag length cap is 100 chars — keep this short.
        tag = f"stage:3|Destination|Redshift {dest_detail}"[:100]
        return [
            "stage:1|Source|DHIS2 API (metadata)",
            "stage:2|S3 Raw|JSONL",
            tag,
        ]

    if dag_type == "okr_redshift_to_rds":
        views = config.get("source", {}).get("views", [])
        target = config.get("target", {})
        source_detail = ", ".join(
            f"{v.get('database', '')}.{v.get('schema', 'public')}.{v['view']}"
            for v in views
        )
        dest_detail = ", ".join(
            f"{target.get('dbname', '')}.{target.get('schema', 'public')}.{v.get('target_table', '')}"
            for v in views
        )
        return [
            f"stage:1|Source|Redshift {source_detail}",
            f"stage:2|Destination|RDS PostgreSQL {dest_detail}",
        ]
    else:
        source_cfg = config.get("source", {})
        source_type = source_cfg.get("type", "unknown")
        source_label = _SOURCE_LABELS.get(source_type, source_type)
        form_id = (source_cfg.get("config") or {}).get("form_id", "")
        source_detail = f"{source_label} (form: {form_id})" if form_id else source_label

        raw_format = config.get("raw", {}).get("format", "json").upper()
        rs = config.get("redshift", {})
        rs_db = rs.get("database", "")
        rs_schema = rs.get("schema", "public")
        rs_table = rs.get("table", "")
        dest_detail = f"{rs_db}.{rs_schema}.{rs_table}" if rs_table else rs_db

        return [
            f"stage:1|Source|{source_detail}",
            f"stage:2|S3 Raw|{raw_format}",
            f"stage:3|S3 Processed|Parquet",
            f"stage:4|S3 Curated|Parquet",
            f"stage:5|Destination|Redshift {dest_detail}",
        ]


def get_env_config() -> Dict[str, str]:
    """
    Read environment-level config from env vars.
    These point to existing AWS resources created by the CDK stack.
    """
    env = _get_env("LMD_ENVIRONMENT", "dev")
    project_code = _get_env("LMD_PROJECT_CODE", "lmd-dp-airflow-v1")
    prefix = f"{project_code}-{env}"

    # Environment-specific config (avoids reliance on env vars in MWAA)
    account_id = _get_env("AWS_ACCOUNT_ID", "002190277880")
    ENV_CONFIGS = {
        "dev": {
            "redshift_secret_name": "lmd-20-dev",
        },
    }
    env_cfg = ENV_CONFIGS.get(env, ENV_CONFIGS["dev"])

    return {
        "environment": env,
        "project_code": project_code,
        "prefix": prefix,
        # S3 buckets (convention: {prefix}-{layer})
        "raw_bucket": f"{prefix}-raw",
        "processed_bucket": f"{prefix}-processed",
        "curated_bucket": f"{prefix}-curated",
        "assets_bucket": f"{prefix}-assets",
        # DynamoDB
        "metadata_table": f"{prefix}-pipeline-metadata",
        # Glue
        "glue_database": f"{prefix}".replace("-", "_"),
        # Redshift
        "redshift_secret_name": env_cfg["redshift_secret_name"],
        "redshift_iam_role_arn": f"arn:aws:iam::{account_id}:role/{prefix}-redshift-spectrum-role",
        "redshift_database": _get_env("REDSHIFT_DATABASE", f"{prefix}".replace("-", "_")),
        # Region
        "aws_region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    }
