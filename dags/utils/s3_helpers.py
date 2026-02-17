"""
S3 helper utilities for reading/writing pipeline data.
"""
import json
import boto3
from datetime import datetime
from typing import Dict, Any


def build_raw_path(
    country: str,
    pipeline_name: str,
    execution_id: str,
    ingestion_time: str,
) -> str:
    """
    Build S3 key for Raw layer data.

    Format: {country}/{pipeline}/{YYYY}/{MM}/{DD}/{execution_id}/

    Example: liberia/ifi/2024/06/15/abc-123-def/
    """
    ts = datetime.fromisoformat(ingestion_time)
    return (
        f"{country}/{pipeline_name}/"
        f"{ts.strftime('%Y/%m/%d')}/"
        f"{execution_id}/"
    )


def write_json_to_s3(
    bucket: str,
    key: str,
    data: Any,
) -> str:
    """
    Write data as JSON to S3.

    Args:
        bucket: S3 bucket name
        key: S3 object key
        data: Data to serialize as JSON

    Returns:
        Full S3 path (s3://bucket/key)
    """
    s3 = boto3.client("s3")
    body = json.dumps(data, default=str, ensure_ascii=False)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"


def read_json_from_s3(bucket: str, key: str) -> Any:
    """Read and parse JSON from S3."""
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def read_sql_from_s3(s3_path: str) -> str:
    """
    Read a SQL file from S3.

    Args:
        s3_path: Full s3:// path or bucket/key

    Returns:
        SQL content as string
    """
    if s3_path.startswith("s3://"):
        path = s3_path[5:]
        bucket, key = path.split("/", 1)
    else:
        bucket, key = s3_path.split("/", 1)

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")


def upload_file_to_s3(local_path: str, bucket: str, key: str) -> str:
    """Upload a local file to S3."""
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"
