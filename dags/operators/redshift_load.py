"""
Redshift loader.

Loads data from Curated S3 (Parquet) into Redshift Serverless.
Supports three load modes:
    - replace: TRUNCATE target table, then COPY
    - append:  COPY directly (new rows added)
    - merge:   COPY into staging, DELETE+INSERT (upsert by merge keys)

Uses Redshift Data API (async) for serverless workgroups.
"""
import time
import json
import boto3
from typing import Dict, Any


def execute_load(
    config: Dict,
    env_config: Dict,
    curated_s3_path: str,
    load_params: Dict,
) -> Dict:
    """
    Load data from Curated S3 into Redshift.

    Args:
        config: Pipeline YAML config
        env_config: Environment config
        curated_s3_path: S3 path to curated Parquet data
        load_params: Execution parameters

    Returns:
        Dict with status, rows_loaded, max_watermark
    """
    redshift_cfg = config["redshift"]
    schema = redshift_cfg["schema"]
    table = redshift_cfg["table"]
    load_mode = redshift_cfg.get("load_mode", "replace")
    merge_keys = redshift_cfg.get("merge_keys", [])
    iam_role = env_config["redshift_iam_role_arn"]
    database = redshift_cfg.get("database") or env_config["redshift_database"]

    # Fetch Redshift connection details from Secrets Manager
    secret_arn, secret_values = _get_redshift_secret(env_config["redshift_secret_name"], env_config["aws_region"])
    workgroup = secret_values["workgroupName"]

    client = boto3.client("redshift-data", region_name=env_config["aws_region"])

    # Auto-create Redshift table from Glue catalog schema if it doesn't exist
    _ensure_table_exists(
        client, workgroup, database, secret_arn,
        schema, table,
        env_config["glue_database"], config["pipeline"]["name"],
        env_config["aws_region"],
    )

    # Run pre-SQL if configured
    pre_sql = redshift_cfg.get("pre_sql")
    if pre_sql:
        _execute_sql(client, workgroup, database, secret_arn, pre_sql)

    if load_mode == "replace":
        _load_replace(client, workgroup, database, secret_arn, schema, table, curated_s3_path, iam_role)

    elif load_mode == "append":
        _load_append(client, workgroup, database, secret_arn, schema, table, curated_s3_path, iam_role)

    elif load_mode == "merge":
        if not merge_keys:
            raise ValueError("merge_keys required for load_mode=merge")
        _load_merge(client, workgroup, database, secret_arn, schema, table, curated_s3_path, iam_role, merge_keys)

    else:
        raise ValueError(f"Unknown load_mode: {load_mode}")

    # Run post-SQL if configured
    post_sql = redshift_cfg.get("post_sql")
    if post_sql:
        _execute_sql(client, workgroup, database, secret_arn, post_sql)

    return {
        "status": "loaded",
        "load_mode": load_mode,
        "table": f"{schema}.{table}",
        "source": curated_s3_path,
        "max_watermark": load_params.get("start_after"),
    }


_GLUE_TO_REDSHIFT_TYPES = {
    "string": "VARCHAR(65535)",
    "int": "INTEGER",
    "bigint": "BIGINT",
    "long": "BIGINT",
    "double": "DOUBLE PRECISION",
    "float": "REAL",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "decimal": "DECIMAL(38,10)",
    "binary": "VARCHAR(65535)",
}


def _glue_type_to_redshift(glue_type):
    """Map a Glue Data Catalog type to a Redshift column type."""
    glue_type = glue_type.lower()
    if glue_type in _GLUE_TO_REDSHIFT_TYPES:
        return _GLUE_TO_REDSHIFT_TYPES[glue_type]
    if glue_type.startswith("decimal"):
        return glue_type.upper()
    if glue_type.startswith("array") or glue_type.startswith("struct") or glue_type.startswith("map"):
        return "SUPER"
    return "VARCHAR(65535)"


def _ensure_table_exists(client, workgroup, database, secret_arn, schema, table, glue_database, pipeline_name, region):
    """Create the Redshift table from the Glue catalog schema if it doesn't exist."""
    # Check if table already exists
    check_sql = f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """
    result = _execute_sql(client, workgroup, database, secret_arn, check_sql)
    if result.get("ResultRows", 0) > 0:
        return

    # Read schema from Glue Data Catalog
    glue_client = boto3.client("glue", region_name=region)

    # Crawler uses table prefix "{pipeline_name}_", so the table name in Glue
    # is typically "{pipeline_name}_{path_segment}" — list tables to find it
    response = glue_client.get_tables(
        DatabaseName=glue_database,
        Expression=f"{pipeline_name}_*",
    )

    if not response["TableList"]:
        raise RuntimeError(
            f"No Glue catalog table found matching '{pipeline_name}_*' in database '{glue_database}'. "
            f"Ensure the Glue Crawler has run."
        )

    # Use the most recently updated table
    glue_table = sorted(response["TableList"], key=lambda t: t.get("UpdateTime", ""), reverse=True)[0]
    columns = glue_table["StorageDescriptor"]["Columns"]

    # Build CREATE TABLE DDL
    col_defs = []
    for col in columns:
        rs_type = _glue_type_to_redshift(col["Type"])
        col_defs.append(f'    "{col["Name"]}" {rs_type}')

    ddl = f'CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{",".join(col_defs)}\n);'
    print(f"Auto-creating Redshift table {schema}.{table} from Glue catalog")
    _execute_sql(client, workgroup, database, secret_arn, ddl)


def _get_redshift_secret(secret_name, region):
    """Fetch Redshift secret from Secrets Manager. Returns (arn, secret_dict)."""
    sm_client = boto3.client("secretsmanager", region_name=region)
    response = sm_client.get_secret_value(SecretId=secret_name)
    return response["ARN"], json.loads(response["SecretString"])


def _load_replace(client, workgroup, database, secret_arn, schema, table, s3_path, iam_role):
    """TRUNCATE + COPY."""
    sql = f"""
        BEGIN;
        TRUNCATE TABLE {schema}.{table};
        COPY {schema}.{table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
        COMMIT;
    """
    _execute_sql(client, workgroup, database, secret_arn, sql)


def _load_append(client, workgroup, database, secret_arn, schema, table, s3_path, iam_role):
    """Direct COPY (append)."""
    sql = f"""
        COPY {schema}.{table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """
    _execute_sql(client, workgroup, database, secret_arn, sql)


def _load_merge(client, workgroup, database, secret_arn, schema, table, s3_path, iam_role, merge_keys):
    """COPY into staging → DELETE matching → INSERT from staging."""
    staging_table = f"{table}_staging_{int(time.time())}"
    join_condition = " AND ".join(
        f"{schema}.{table}.{k} = {staging_table}.{k}" for k in merge_keys
    )

    sql = f"""
        BEGIN;

        CREATE TEMP TABLE {staging_table} (LIKE {schema}.{table});

        COPY {staging_table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;

        DELETE FROM {schema}.{table}
        USING {staging_table}
        WHERE {join_condition};

        INSERT INTO {schema}.{table}
        SELECT * FROM {staging_table};

        DROP TABLE {staging_table};

        COMMIT;
    """
    _execute_sql(client, workgroup, database, secret_arn, sql)


def _execute_sql(client, workgroup, database, secret_arn, sql):
    """Execute SQL on Redshift Serverless via Data API and wait for completion."""
    response = client.execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        SecretArn=secret_arn,
        Sql=sql,
    )
    statement_id = response["Id"]

    # Poll for completion
    max_attempts = 120
    for _ in range(max_attempts):
        status = client.describe_statement(Id=statement_id)
        state = status["Status"]

        if state == "FINISHED":
            return status
        if state in ("FAILED", "ABORTED"):
            error = status.get("Error", "Unknown error")
            raise RuntimeError(f"Redshift query failed: {error}\nSQL: {sql[:500]}")

        time.sleep(5)

    raise TimeoutError(f"Redshift query timed out after {max_attempts * 5}s")
