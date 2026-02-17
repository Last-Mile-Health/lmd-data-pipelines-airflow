"""
Kobo Toolbox API ingestor.

Pulls form submissions from Kobo API and writes raw JSON to Raw layer (S3).
Supports incremental loads via _submission_time filtering.
"""
import json
import requests
import boto3
from typing import Dict, Any

from utils.s3_helpers import build_raw_path, write_json_to_s3


def run(config: Dict, env_config: Dict, load_params: Dict) -> Dict:
    """
    Ingest data from Kobo Toolbox API → Raw S3.

    Args:
        config: Pipeline YAML config
        env_config: Environment config (buckets, tables, etc.)
        load_params: Load parameters (execution_id, mode, watermark, etc.)

    Returns:
        Dict with s3_path, record_count, max_watermark
    """
    source_cfg = config["source"]["config"]
    secret_name = config["source"].get("secret_name")

    print(f"secret name : {secret_name}")

    # Fetch API credentials from Secrets Manager
    secrets = _get_secrets(secret_name)
    base_url = secrets[source_cfg["base_url_key"]].rstrip("/")
    token = secrets[source_cfg["auth_token_key"]]

    form_id = source_cfg["form_id"]
    export_format = source_cfg.get("export_format", "json")

    # Build API URL
    url = f"{base_url}/assets/{form_id}/data.{export_format}"

    print(f"url", url)
    print(f"config", config)
    # Set up headers
    headers = {
        "Authorization": f"Token {token}",
        "Accept": "application/json",
    }

    # Fetch data (with pagination)
    all_results = []
    params = {"format": "json", "limit": 1000}

    # Incremental: filter by submission time
    if load_params["mode"] == "incremental" and load_params.get("start_after"):
        params["query"] = json.dumps({
            "_submission_time": {"$gt": load_params["start_after"]}
        })

    next_url = url
    while next_url:
        response = requests.get(next_url, headers=headers, params=params, timeout=300)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", data if isinstance(data, list) else [data])
        all_results.extend(results)

        next_url = data.get("next")
        params = {}  # next_url already includes params

    # Compute max watermark for incremental
    incremental_key = load_params.get("incremental_key")
    max_watermark = None
    if incremental_key and all_results:
        watermark_values = [
            r.get(incremental_key) for r in all_results
            if r.get(incremental_key)
        ]
        if watermark_values:
            max_watermark = max(watermark_values)


        print(f'----- watermark', watermark_values)

    # Write to Raw (S3 as JSON)
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
        data=all_results,
    )

    return {
        "s3_path": s3_path,
        "s3_key": s3_key,
        "record_count": len(all_results),
        "max_watermark": max_watermark,
        "source_type": "kobo_api",
    }


def _get_secrets(secret_name: str) -> Dict:
    """Fetch secrets from AWS Secrets Manager."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])
