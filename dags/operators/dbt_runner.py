"""
Redshift view transformer.

Executes CREATE OR REPLACE VIEW statements on Redshift
to refresh transform views before extraction.

Uses the same Redshift Data API helpers as redshift_load.py.
"""
import os
import glob
from typing import Dict, Any, Optional

import boto3

from operators.redshift_load import _get_redshift_secret, _execute_sql


def run_transforms(
    config: Dict,
    env_config: Dict,
    sql_dir: str,
    select: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute all SQL transform files in the given directory on Redshift.

    SQL files should contain CREATE OR REPLACE VIEW (or any valid Redshift SQL).
    Files are executed in alphabetical order.

    Args:
        config: Pipeline YAML config
        env_config: Environment config
        sql_dir: Relative path to directory containing .sql files (from repo root)
        select: Optional glob pattern to filter SQL files (e.g. "example_1")

    Returns:
        Dict with status and list of executed files
    """
    redshift_cfg = config.get("redshift", {})
    database = redshift_cfg.get("database") or env_config["redshift_database"]

    secret_arn, secret_values = _get_redshift_secret(
        env_config["redshift_secret_name"], env_config["aws_region"]
    )
    workgroup = secret_values["workgroupName"]
    client = boto3.client("redshift-data", region_name=env_config["aws_region"])

    # Resolve SQL directory — works in MWAA (dags/sql/), Docker (../../sql/), and local
    dags_dir = os.path.dirname(os.path.dirname(__file__))
    _mwaa_path = os.path.join(dags_dir, sql_dir)
    _local_path = os.path.join(dags_dir, "..", sql_dir)
    abs_sql_dir = _mwaa_path if os.path.isdir(_mwaa_path) else _local_path

    # Find SQL files
    pattern = os.path.join(abs_sql_dir, f"*{select}*.sql" if select else "*.sql")
    sql_files = sorted(glob.glob(pattern))

    if not sql_files:
        print(f"[OKR Transform] No SQL files found in {abs_sql_dir}")
        return {"status": "skipped", "files_executed": []}

    executed = []
    for sql_file in sql_files:
        filename = os.path.basename(sql_file)
        with open(sql_file) as f:
            sql = f.read().strip()

        if not sql:
            print(f"[OKR Transform] Skipping empty file: {filename}")
            continue

        print(f"[OKR Transform] Executing: {filename}")
        _execute_sql(client, workgroup, database, secret_arn, sql)
        executed.append(filename)
        print(f"[OKR Transform] Completed: {filename}")

    return {"status": "success", "files_executed": executed}
