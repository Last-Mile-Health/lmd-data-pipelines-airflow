"""
Pipeline configuration loader.

Reads YAML configs from config/pipelines/ and provides them to the DAG factory.
Each YAML file defines one pipeline (source, layers, schedule, load mode, etc).
"""
import os
import re
import glob
import yaml
from typing import Dict, Any


CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "config", "pipelines")

_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")


def _resolve_env_vars(obj):
    """Recursively resolve ${VAR_NAME} placeholders in config values."""
    if isinstance(obj, str):
        return _ENV_VAR_PATTERN.sub(
            lambda m: os.environ.get(m.group(1), m.group(0)), obj
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


def load_all_pipeline_configs() -> Dict[str, Dict[str, Any]]:
    """
    Load all pipeline configs from config/pipelines/*.yml.

    Returns:
        Dict mapping pipeline_name -> config dict
    """
    configs = {}
    pattern = os.path.join(CONFIG_DIR, "*.yml")
    for filepath in sorted(glob.glob(pattern)):
        with open(filepath) as f:
            config = _resolve_env_vars(yaml.safe_load(f))
            if config and "pipeline" in config:
                name = config["pipeline"]["name"]
                configs[name] = config
    return configs


def get_env_config() -> Dict[str, str]:
    """
    Read environment-level config from env vars.
    These point to existing AWS resources created by the CDK stack.
    """
    env = os.getenv("LMD_ENVIRONMENT", "dev")
    project_code = os.getenv("LMD_PROJECT_CODE", "lmd-dp-airflow-v1")
    prefix = f"{project_code}-{env}"

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
        "redshift_secret_name": os.getenv("REDSHIFT_SECRET_NAME", ""),
        "redshift_iam_role_arn": os.getenv("REDSHIFT_IAM_ROLE_ARN", ""),
        "redshift_database": os.getenv("REDSHIFT_DATABASE", f"{prefix}".replace("-", "_")),
        # Region
        "aws_region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    }
