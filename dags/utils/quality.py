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
