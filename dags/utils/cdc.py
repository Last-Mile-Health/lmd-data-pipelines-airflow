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

    # Only update watermark if we have an actual max value from the data.
    # Using ingestion_time as fallback would skip all older records on next run.
    if not max_watermark:
        print(f"[CDC] No max_watermark provided — keeping existing watermark unchanged")
        return

    table.put_item(
        Item={
            "pipeline_name": pipeline_name,
            "execution_id": "watermark",
            "last_watermark_value": max_watermark,
            "last_successful_run": datetime.utcnow().isoformat(),
            "last_execution_id": execution_id,
        }
    )


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
