"""
CSV file ingestor.

Picks up CSV files from an S3 prefix (uploaded manually or by external systems),
converts to JSON, and writes to Raw layer.

For "full" mode: processes all files in the prefix.
For "incremental" mode: processes only files modified after the last watermark.
"""
import csv
import io
import json
import boto3
from typing import Dict, Any, List
from datetime import datetime

from utils.s3_helpers import build_raw_path, write_json_to_s3


def run(config: Dict, env_config: Dict, load_params: Dict) -> Dict:
    """
    Ingest CSV files from S3 prefix → Raw S3 (as JSON).

    Config options (source.config):
        s3_prefix: S3 prefix where CSVs land (e.g., "uploads/facility_list/")
        file_pattern: Glob pattern (default: "*.csv")
        delimiter: CSV delimiter (default: ",")
        encoding: File encoding (default: "utf-8")
        source_bucket: Optional override bucket (default: raw_bucket)
    """
    source_cfg = config["source"]["config"]
    s3 = boto3.client("s3")

    source_bucket = source_cfg.get("source_bucket", env_config["raw_bucket"])
    prefix = source_cfg.get("s3_prefix", "").strip("/") + "/"
    delimiter = source_cfg.get("delimiter", ",")
    encoding = source_cfg.get("encoding", "utf-8")

    # List CSV files in prefix
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=prefix)
    objects = response.get("Contents", [])

    # Filter for CSV files
    csv_files = [
        obj for obj in objects
        if obj["Key"].lower().endswith(".csv") and obj["Size"] > 0
    ]

    # Incremental: only files modified after watermark
    if load_params["mode"] == "incremental" and load_params.get("start_after"):
        watermark_time = datetime.fromisoformat(load_params["start_after"])
        csv_files = [
            obj for obj in csv_files
            if obj["LastModified"].replace(tzinfo=None) > watermark_time
        ]

    if not csv_files:
        return {
            "s3_path": None,
            "record_count": 0,
            "max_watermark": load_params.get("start_after"),
            "source_type": "csv",
            "message": "No new CSV files found",
        }

    # Read and merge all CSV files
    all_records = []
    latest_modified = None

    for obj in csv_files:
        key = obj["Key"]
        response = s3.get_object(Bucket=source_bucket, Key=key)
        content = response["Body"].read().decode(encoding)

        reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
        records = list(reader)
        all_records.extend(records)

        # Track latest modification for watermark
        modified = obj["LastModified"].replace(tzinfo=None)
        if latest_modified is None or modified > latest_modified:
            latest_modified = modified

    max_watermark = latest_modified.isoformat() if latest_modified else None

    # Write to Raw (as JSON)
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
        data=all_records,
    )

    return {
        "s3_path": s3_path,
        "s3_key": s3_key,
        "record_count": len(all_records),
        "max_watermark": max_watermark,
        "source_type": "csv",
        "files_processed": len(csv_files),
    }
