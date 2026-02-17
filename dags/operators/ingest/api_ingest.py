"""
Generic REST API ingestor.

Pulls data from any REST API and writes raw JSON to Raw (S3).
Supports: offset, cursor, and page-based pagination.
"""
import json
import requests
import boto3
from typing import Dict, Any

from utils.s3_helpers import build_raw_path, write_json_to_s3


def run(config: Dict, env_config: Dict, load_params: Dict) -> Dict:
    """
    Ingest data from a generic REST API → Raw S3.

    Config options (source.config):
        base_url: API base URL
        endpoint: API path
        method: GET | POST (default: GET)
        headers: Additional headers
        params: Query parameters
        body: Request body (for POST)
        pagination:
            type: offset | cursor | page | none
            limit_param: name of limit param (default: limit)
            offset_param: name of offset param (default: offset)
            cursor_param: name of cursor param
            cursor_path: JSON path to next cursor in response
            page_size: records per page (default: 1000)
        data_path: JSON path to data array in response (default: root)
    """
    source_cfg = config["source"]["config"]
    secret_name = config["source"].get("secret_name")

    # Fetch credentials if needed
    secrets = _get_secrets(secret_name) if secret_name else {}

    base_url = source_cfg.get("base_url", secrets.get("API_BASE_URL", "")).rstrip("/")
    endpoint = source_cfg.get("endpoint", "")
    method = source_cfg.get("method", "GET").upper()
    url = f"{base_url}{endpoint}"

    # Build headers
    req_headers = {"Accept": "application/json"}
    req_headers.update(source_cfg.get("headers", {}))

    # Auth token from secrets
    if secrets.get("API_TOKEN"):
        req_headers["Authorization"] = f"Bearer {secrets['API_TOKEN']}"
    elif secrets.get("API_KEY"):
        req_headers["X-API-Key"] = secrets["API_KEY"]

    # Query params
    params = dict(source_cfg.get("params", {}))

    # Incremental filter
    if load_params["mode"] == "incremental" and load_params.get("start_after"):
        incremental_key = load_params.get("incremental_key", "updated_at")
        params[f"{incremental_key}_gte"] = load_params["start_after"]

    # Pagination config
    pagination = source_cfg.get("pagination", {"type": "none"})
    page_type = pagination.get("type", "none")
    page_size = pagination.get("page_size", 1000)
    data_path = source_cfg.get("data_path")

    all_data = []

    if page_type == "offset":
        limit_param = pagination.get("limit_param", "limit")
        offset_param = pagination.get("offset_param", "offset")
        params[limit_param] = page_size
        offset = 0

        while True:
            params[offset_param] = offset
            data = _make_request(method, url, req_headers, params, source_cfg.get("body"))
            records = _extract_data(data, data_path)
            all_data.extend(records)
            if len(records) < page_size:
                break
            offset += page_size

    elif page_type == "cursor":
        cursor_param = pagination.get("cursor_param", "cursor")
        cursor_path = pagination.get("cursor_path", "next_cursor")

        while True:
            data = _make_request(method, url, req_headers, params, source_cfg.get("body"))
            records = _extract_data(data, data_path)
            all_data.extend(records)

            next_cursor = _get_nested(data, cursor_path)
            if not next_cursor or not records:
                break
            params[cursor_param] = next_cursor

    elif page_type == "page":
        page_param = pagination.get("page_param", "page")
        params["per_page"] = page_size
        page_num = 1

        while True:
            params[page_param] = page_num
            data = _make_request(method, url, req_headers, params, source_cfg.get("body"))
            records = _extract_data(data, data_path)
            all_data.extend(records)
            if len(records) < page_size:
                break
            page_num += 1

    else:
        data = _make_request(method, url, req_headers, params, source_cfg.get("body"))
        records = _extract_data(data, data_path)
        all_data.extend(records)

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
        "source_type": "api",
    }


def _make_request(method, url, headers, params, body=None):
    """Execute HTTP request and return parsed JSON."""
    if method == "POST":
        response = requests.post(url, headers=headers, params=params, json=body, timeout=300)
    else:
        response = requests.get(url, headers=headers, params=params, timeout=300)
    response.raise_for_status()
    return response.json()


def _extract_data(data, data_path=None):
    """Extract records from response using data_path."""
    if data_path:
        return _get_nested(data, data_path) or []
    if isinstance(data, list):
        return data
    if "results" in data:
        return data["results"]
    if "data" in data:
        return data["data"]
    return [data]


def _get_nested(obj, path):
    """Get nested value from dict using dot notation: 'response.data.items'."""
    if not path:
        return obj
    for key in path.split("."):
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return None
    return obj


def _get_secrets(secret_name: str) -> Dict:
    """Fetch secrets from AWS Secrets Manager."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])
