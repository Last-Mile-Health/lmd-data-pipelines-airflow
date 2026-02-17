"""
DHIS2 API ingestor.

Pulls analytics/data values from a DHIS2 instance and writes raw JSON to Raw (S3).
Supports multiple DHIS2 endpoints: analytics, dataValueSets, events, trackedEntityInstances.
"""
import json
import requests
import boto3
from typing import Dict, Any

from utils.s3_helpers import build_raw_path, write_json_to_s3


def run(config: Dict, env_config: Dict, load_params: Dict) -> Dict:
    """
    Ingest data from DHIS2 API → Raw S3.

    Config options (source.config):
        base_url: DHIS2 instance URL
        endpoint: API endpoint (e.g., /api/analytics, /api/dataValueSets)
        dimensions: List of dimension strings for analytics
        params: Additional query parameters
        auth_type: basic | token (default: basic)
    """
    source_cfg = config["source"]["config"]
    secret_name = config["source"].get("secret_name")

    # Fetch credentials
    secrets = _get_secrets(secret_name) if secret_name else {}

    base_url = source_cfg.get("base_url", secrets.get("DHIS2_BASE_URL", "")).rstrip("/")
    endpoint = source_cfg.get("endpoint", "/api/analytics")
    auth_type = source_cfg.get("auth_type", "basic")

    # Set up auth
    auth = None
    headers = {"Accept": "application/json"}
    if auth_type == "basic":
        auth = (secrets.get("DHIS2_USERNAME", ""), secrets.get("DHIS2_PASSWORD", ""))
    elif auth_type == "token":
        headers["Authorization"] = f"Bearer {secrets.get('DHIS2_TOKEN', '')}"

    # Build query params
    params = dict(source_cfg.get("params", {}))

    # Analytics dimensions
    dimensions = source_cfg.get("dimensions", [])
    if dimensions:
        for dim in dimensions:
            params.setdefault("dimension", [])
            if isinstance(params["dimension"], list):
                params["dimension"].append(dim)

    # Incremental: filter by period or lastUpdated
    if load_params["mode"] == "incremental" and load_params.get("start_after"):
        params["lastUpdated"] = load_params["start_after"]

    # Fetch data
    url = f"{base_url}{endpoint}"
    all_data = []

    # Handle pagination
    page = 1
    page_size = source_cfg.get("page_size", 1000)
    params["pageSize"] = page_size
    params["page"] = page

    while True:
        response = requests.get(url, headers=headers, auth=auth, params=params, timeout=300)
        response.raise_for_status()
        data = response.json()

        # DHIS2 analytics returns rows, dataValueSets returns dataValues
        if "rows" in data:
            headers_list = data.get("headers", [])
            header_names = [h.get("name", h.get("column", f"col_{i}")) for i, h in enumerate(headers_list)]
            rows = [dict(zip(header_names, row)) for row in data["rows"]]
            all_data.extend(rows)
        elif "dataValues" in data:
            all_data.extend(data["dataValues"])
        elif "trackedEntityInstances" in data:
            all_data.extend(data["trackedEntityInstances"])
        elif "events" in data:
            all_data.extend(data["events"])
        elif isinstance(data, list):
            all_data.extend(data)
        else:
            all_data.append(data)

        # Check pagination
        pager = data.get("pager", {})
        if pager.get("page", 1) >= pager.get("pageCount", 1):
            break
        page += 1
        params["page"] = page

    # Compute max watermark
    incremental_key = load_params.get("incremental_key")
    max_watermark = None
    if incremental_key and all_data:
        watermark_values = [r.get(incremental_key) for r in all_data if r.get(incremental_key)]
        if watermark_values:
            max_watermark = max(watermark_values)

    # Write to Raw
    raw_key = build_raw_path(
        country=load_params["country"],
        pipeline_name=load_params["pipeline_name"],
        execution_id=load_params["execution_id"],
        ingestion_time=load_params["ingestion_time"],
    )
    s3_key = f"{raw_key}data.json"
    s3_path = write_json_to_s3(
        bucket=env_config["raw_bucket"],
        key=s3_key,
        data=all_data,
    )

    return {
        "s3_path": s3_path,
        "s3_key": s3_key,
        "record_count": len(all_data),
        "max_watermark": max_watermark,
        "source_type": "dhis2",
    }


def _get_secrets(secret_name: str) -> Dict:
    """Fetch secrets from AWS Secrets Manager."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])
