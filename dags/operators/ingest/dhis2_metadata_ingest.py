"""
DHIS2 metadata (dimension) ingestor.

Fetches metadata from a DHIS2 instance (e.g. dataElements, organisationUnits,
categoryOptionCombos) and writes flattened JSON to the Raw layer in S3.

Used by the dedicated DHIS2 DAG for loading dimension tables.
"""
import json
import requests
import boto3
from typing import Dict, Any, List

from utils.s3_helpers import build_dimension_raw_path, write_jsonl_to_s3
from operators.ingest.dhis2_ingest import build_retrying_session


def run_dimension(
    dimension_cfg: Dict,
    source_cfg: Dict,
    secret_name: str,
    env_config: Dict,
    load_params: Dict,
) -> Dict:
    """
    Fetch one DHIS2 metadata endpoint, flatten nested fields, write JSON to S3.

    Args:
        dimension_cfg: Single entry from config['dimensions'] list
        source_cfg: config['source']['config']
        secret_name: AWS Secrets Manager secret name for DHIS2 credentials
        env_config: Environment config (buckets, region, etc.)
        load_params: Execution parameters (execution_id, ingestion_time, country, etc.)

    Returns:
        Dict with s3_path, record_count, dim_name
    """
    from operators.ingest.dhis2_ingest import _get_secrets

    secrets = _get_secrets(secret_name) if secret_name else {}
    base_url = source_cfg.get("base_url", secrets.get("DHIS2_BASE_URL", "")).rstrip("/")
    auth_type = source_cfg.get("auth_type", "basic")

    # Auth
    auth = None
    headers = {"Accept": "application/json"}
    if auth_type == "basic":
        auth = (secrets.get("DHIS2_USERNAME", ""), secrets.get("DHIS2_PASSWORD", ""))
    elif auth_type == "token":
        headers["Authorization"] = f"Bearer {secrets.get('DHIS2_TOKEN', '')}"

    # Build request
    endpoint = dimension_cfg["endpoint"]
    fields = dimension_cfg.get("fields", "")
    response_key = dimension_cfg["response_key"]

    params = {"paging": "false"}
    if fields:
        params["fields"] = fields

    url = f"{base_url}{endpoint}"
    print(f"[DIM] Fetching {dimension_cfg['name']} from {url}")

    session = build_retrying_session()
    response = session.get(url, headers=headers, auth=auth, params=params, timeout=300)
    response.raise_for_status()
    data = response.json()

    # Extract records using the response_key
    records = data.get(response_key, [])

    # Flatten nested objects (e.g. categoryCombo.id → categorycombo_id)
    flattened = [_flatten_record(r) for r in records]

    print(f"[DIM] {dimension_cfg['name']}: {len(flattened)} records fetched")

    # Write to S3
    dim_name = dimension_cfg["name"]
    raw_key = build_dimension_raw_path(
        country=load_params["country"],
        pipeline_name=load_params["pipeline_name"],
        dim_name=dim_name,
        execution_id=load_params["execution_id"],
        ingestion_time=load_params["ingestion_time"],
    )
    s3_key = f"{raw_key}data.jsonl"
    s3_path = write_jsonl_to_s3(
        bucket=env_config["raw_bucket"],
        key=s3_key,
        data=flattened,
    )

    return {
        "s3_path": s3_path,
        "s3_key": s3_key,
        "record_count": len(flattened),
        "dim_name": dim_name,
    }


def _flatten_record(record: Dict, parent_key: str = "", sep: str = "_") -> Dict:
    """
    Flatten nested dicts into a single level.

    Example:
        {"id": "abc", "categoryCombo": {"id": "x", "displayName": "Y"}}
        → {"id": "abc", "categorycombo_id": "x", "categorycombo_displayname": "y"}

    All keys are lowercased to match Redshift column conventions.
    """
    items = {}
    for k, v in record.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        new_key = new_key.lower()
        if isinstance(v, dict):
            items.update(_flatten_record(v, new_key, sep))
        elif isinstance(v, list):
            # Store lists as JSON strings
            items[new_key] = json.dumps(v, default=str)
        else:
            items[new_key] = v
    return items
