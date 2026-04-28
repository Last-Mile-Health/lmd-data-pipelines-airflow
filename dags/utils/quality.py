"""
Data quality check utilities.

Runs post-load validation against Redshift.
"""
import boto3
import json
import time
from typing import Dict, List, Any

from operators.redshift_load import _get_redshift_secret


def run_checks(checks: List[Dict], config: Dict, env_config: Dict) -> Dict:
    """
    Execute configured quality checks against Redshift.

    Supported check types:
        - row_count_min: Assert minimum row count
        - null_check: Assert no nulls in specified columns
        - null_pct_max: Assert NULL pct in column is below threshold (0.0-1.0)
        - freshness_max_hours: Assert max(timestamp_column) is within N hours of now
        - custom_sql: Run arbitrary SQL and compare result

    Returns:
        Dict with status and results per check
    """
    redshift = boto3.client("redshift-data", region_name=env_config["aws_region"])
    secret_arn, secret_values = _get_redshift_secret(env_config["redshift_secret_name"], env_config["aws_region"])
    workgroup = secret_values["workgroupName"]
    database = config["redshift"].get("database") or env_config["redshift_database"]
    schema = config["redshift"]["schema"]
    table = config["redshift"]["table"]
    results = []

    for check in checks:
        check_type = check["type"]
        result = {"type": check_type, "passed": False}

        if check_type == "row_count_min":
            sql = f"SELECT COUNT(*) as cnt FROM {schema}.{table}"
            count = _execute_and_fetch(redshift, sql, workgroup, database, secret_arn)
            threshold = check.get("threshold", 1)
            result["actual"] = count
            result["threshold"] = threshold
            result["passed"] = count >= threshold

        elif check_type == "null_check":
            columns = check.get("columns", [])
            for col in columns:
                sql = f"SELECT COUNT(*) as cnt FROM {schema}.{table} WHERE {col} IS NULL"
                null_count = _execute_and_fetch(redshift, sql, workgroup, database, secret_arn)
                col_result = {
                    "column": col,
                    "null_count": null_count,
                    "passed": null_count == 0,
                }
                if not col_result["passed"]:
                    result["passed"] = False
                results.append({**result, **col_result})
            continue

        elif check_type == "null_pct_max":
            column = check["column"]
            threshold = float(check.get("threshold", 0.05))
            sql = (
                f"SELECT 1.0 * SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) "
                f"FROM {schema}.{table}"
            )
            pct = _execute_and_fetch_float(redshift, sql, workgroup, database, secret_arn)
            result["column"] = column
            result["actual_pct"] = pct
            result["threshold_pct"] = threshold
            result["passed"] = pct is not None and pct <= threshold

        elif check_type == "freshness_max_hours":
            column = check["column"]
            max_hours = float(check.get("max_hours", 48))
            sql = (
                f"SELECT EXTRACT(EPOCH FROM (GETDATE() - MAX({column}::timestamp))) / 3600 "
                f"FROM {schema}.{table}"
            )
            age_hours = _execute_and_fetch_float(redshift, sql, workgroup, database, secret_arn)
            result["column"] = column
            result["age_hours"] = age_hours
            result["max_hours"] = max_hours
            result["passed"] = age_hours is not None and age_hours <= max_hours

        elif check_type == "custom_sql":
            sql = check["query"].format(table=f"{schema}.{table}")
            actual = _execute_and_fetch(redshift, sql, workgroup, database, secret_arn)
            expected = check.get("expected", 0)
            result["actual"] = actual
            result["expected"] = expected
            result["passed"] = actual == expected

        results.append(result)

    failed = [r for r in results if not r.get("passed", True)]
    status = "passed" if not failed else "failed"

    if failed:
        raise ValueError(
            f"Quality checks failed: {json.dumps(failed, default=str)}"
        )

    return {"status": status, "results": results}


def _execute_and_fetch(redshift_client, sql: str, workgroup: str, database: str, secret_arn: str) -> int:
    """Execute SQL on Redshift Serverless and return scalar result."""
    response = redshift_client.execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        SecretArn=secret_arn,
        Sql=sql,
    )
    statement_id = response["Id"]

    # Poll for completion
    while True:
        status = redshift_client.describe_statement(Id=statement_id)
        state = status["Status"]
        if state == "FINISHED":
            break
        if state in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift query failed: {status.get('Error', 'Unknown')}")
        time.sleep(2)

    result = redshift_client.get_statement_result(Id=statement_id)
    if result["Records"]:
        return int(result["Records"][0][0].get("longValue", 0))
    return 0


def _execute_and_fetch_float(redshift_client, sql: str, workgroup: str, database: str, secret_arn: str):
    """Execute SQL and return scalar as float (or None if NULL/empty)."""
    response = redshift_client.execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        SecretArn=secret_arn,
        Sql=sql,
    )
    statement_id = response["Id"]
    while True:
        status = redshift_client.describe_statement(Id=statement_id)
        state = status["Status"]
        if state == "FINISHED":
            break
        if state in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift query failed: {status.get('Error', 'Unknown')}")
        time.sleep(2)

    result = redshift_client.get_statement_result(Id=statement_id)
    if not result["Records"]:
        return None
    cell = result["Records"][0][0]
    if cell.get("isNull"):
        return None
    for k in ("doubleValue", "longValue", "stringValue"):
        if k in cell:
            try:
                return float(cell[k])
            except (TypeError, ValueError):
                return None
    return None
