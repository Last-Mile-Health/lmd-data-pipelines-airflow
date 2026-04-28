"""
Change Data Capture (CDC) utilities.

Manages high-watermark tracking in DynamoDB for incremental loads.

Watermark record structure in DynamoDB:
    PK: pipeline_name
    SK: "watermark"
    last_watermark_value: "2024-06-15T10:30:00Z"
    last_successful_run: "2024-06-16T06:00:00Z"
    last_execution_id: "uuid"
"""
import boto3
from datetime import datetime
from typing import Dict, Optional


def get_watermark(pipeline_name: str, table_name: str) -> Dict:
    """
    Read the last successful watermark for a pipeline.

    Returns:
        Dict with last_value, last_run, last_execution_id.
        All None if no prior run exists.
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    response = table.get_item(
        Key={
            "pipeline_name": pipeline_name,
            "execution_id": "watermark",
        }
    )
    item = response.get("Item", {})

    return {
        "last_value": item.get("last_watermark_value"),
        "last_run": item.get("last_successful_run"),
        "last_execution_id": item.get("last_execution_id"),
    }


def set_watermark(
    pipeline_name: str,
    table_name: str,
    execution_id: str,
    ingestion_time: str,
    max_watermark: Optional[str] = None,
):
    """
    Persist the new watermark after a successful pipeline run.

    Args:
        pipeline_name: Pipeline identifier
        table_name: DynamoDB table name
        execution_id: Current run's execution ID
        ingestion_time: Timestamp of this ingestion
        max_watermark: The maximum value of the incremental key
                       observed in this batch (e.g. max _submission_time)
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    # Always record last_successful_run so operators see the pipeline is alive.
    # Only overwrite last_watermark_value when we have a real max from the data —
    # otherwise we'd skip older records on the next run.
    item = {
        "pipeline_name": pipeline_name,
        "execution_id": "watermark",
        "last_successful_run": datetime.utcnow().isoformat(),
        "last_execution_id": execution_id,
    }

    if max_watermark:
        item["last_watermark_value"] = max_watermark
        table.put_item(Item=item)
    else:
        # Empty window: bump last_successful_run via UpdateExpression, leave value alone.
        print(f"[CDC] No max_watermark — bumping last_successful_run only")
        table.update_item(
            Key={"pipeline_name": pipeline_name, "execution_id": "watermark"},
            UpdateExpression="SET last_successful_run = :ts, last_execution_id = :eid",
            ExpressionAttributeValues={
                ":ts": item["last_successful_run"],
                ":eid": execution_id,
            },
        )


def get_dimension_watermark(pipeline_name: str, dim_name: str, table_name: str) -> Dict:
    """Read last successful refresh for a single dimension."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    response = table.get_item(
        Key={
            "pipeline_name": pipeline_name,
            "execution_id": f"dim_watermark#{dim_name}",
        }
    )
    item = response.get("Item", {})
    return {
        "last_run": item.get("last_successful_run"),
        "last_execution_id": item.get("last_execution_id"),
        "record_count": item.get("record_count"),
    }


def set_dimension_watermark(
    pipeline_name: str,
    dim_name: str,
    table_name: str,
    execution_id: str,
    record_count: int,
):
    """Persist dimension refresh timestamp."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    table.put_item(
        Item={
            "pipeline_name": pipeline_name,
            "execution_id": f"dim_watermark#{dim_name}",
            "dim_name": dim_name,
            "last_successful_run": datetime.utcnow().isoformat(),
            "last_execution_id": execution_id,
            "record_count": record_count,
        }
    )


def dimension_is_fresh(
    pipeline_name: str,
    dim_name: str,
    table_name: str,
    min_refresh_days: int,
) -> bool:
    """True if the dimension was refreshed within the last min_refresh_days."""
    if min_refresh_days <= 0:
        return False
    wm = get_dimension_watermark(pipeline_name, dim_name, table_name)
    last_run = wm.get("last_run")
    if not last_run:
        return False
    try:
        last_dt = datetime.fromisoformat(last_run)
    except (TypeError, ValueError):
        return False
    age_days = (datetime.utcnow() - last_dt).total_seconds() / 86400
    return age_days < min_refresh_days


def compute_boundaries(watermark: Dict, incremental_key: str) -> Dict:
    """
    Compute load boundaries for an incremental run.

    Args:
        watermark: Result from get_watermark()
        incremental_key: Column name used for watermarking

    Returns:
        Dict with incremental_key, start_after (exclusive lower bound)
    """
    return {
        "incremental_key": incremental_key,
        "start_after": watermark.get("last_value"),  # None on first run → pull all
    }
