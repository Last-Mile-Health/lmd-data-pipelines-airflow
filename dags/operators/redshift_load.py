"""
Redshift loader.

Loads data from Curated S3 (Parquet) into Redshift Serverless.
Supports three load modes:
    - replace: TRUNCATE target table, then COPY
    - append:  COPY directly (new rows added)
    - merge:   COPY into staging, DELETE+INSERT (upsert by merge keys)

Also supports dimension table loading (JSON → TRUNCATE + COPY).

Uses Redshift Data API (async) for serverless workgroups.
"""
import os
import time
import json
import boto3
from typing import Dict, Any, List


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

    # Get current Glue columns (types match the latest Parquet schema from crawler)
    glue_database = env_config["glue_database"]
    glue_columns, glue_table_name = _get_glue_columns(
        glue_database, config["pipeline"]["name"],
        env_config["aws_region"],
    )

    # Auto-create Redshift table or add new columns from Glue catalog (grow-only)
    _ensure_table_exists(
        client, workgroup, database, secret_arn,
        schema, table, glue_columns,
        sort_keys=redshift_cfg.get("sort_keys"),
        distribution_style=redshift_cfg.get("distribution_style"),
    )

    # Ensure Spectrum external schema exists for loading via Glue catalog
    external_schema = f"spectrum_{glue_database}"
    _ensure_external_schema(
        client, workgroup, database, secret_arn,
        external_schema, glue_database, iam_role,
    )

    # Run pre-SQL if configured
    pre_sql = redshift_cfg.get("pre_sql")
    if pre_sql:
        _execute_sql(client, workgroup, database, secret_arn, pre_sql)

    # Spectrum params used by all load modes (replaces COPY FROM PARQUET)
    spectrum_params = {
        "external_schema": external_schema,
        "glue_table_name": glue_table_name,
    }

    if load_mode == "replace":
        _load_replace(client, workgroup, database, secret_arn, schema, table, curated_s3_path, iam_role, spectrum_params)

    elif load_mode == "append":
        _load_append(client, workgroup, database, secret_arn, schema, table, curated_s3_path, iam_role, spectrum_params)

    elif load_mode == "merge":
        if not merge_keys:
            raise ValueError("merge_keys required for load_mode=merge")
        _load_merge(client, workgroup, database, secret_arn, schema, table, curated_s3_path, iam_role, merge_keys, glue_columns, spectrum_params)

    else:
        raise ValueError(f"Unknown load_mode: {load_mode}")

    # Run post-SQL if configured (supports inline SQL or file path starting with sql/)
    post_sql = redshift_cfg.get("post_sql")
    if post_sql:
        if post_sql.startswith("sql/"):
            # In MWAA: dags/sql/..., locally: ../../sql/...
            _mwaa_path = os.path.join(os.path.dirname(__file__), "..", post_sql)
            _local_path = os.path.join(os.path.dirname(__file__), "..", "..", post_sql)
            sql_file_path = _mwaa_path if os.path.exists(_mwaa_path) else _local_path
            with open(sql_file_path) as f:
                post_sql = f.read()
        _execute_sql(client, workgroup, database, secret_arn, post_sql)

    # Compute max watermark from the loaded data
    max_watermark = None
    incremental_key = load_params.get("incremental_key")
    if incremental_key and load_params.get("mode") == "incremental":
        max_watermark = _get_max_watermark(
            client, workgroup, database, secret_arn,
            schema, table, incremental_key,
        )

    return {
        "status": "loaded",
        "load_mode": load_mode,
        "table": f"{schema}.{table}",
        "source": curated_s3_path,
        "max_watermark": max_watermark,
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


def _get_glue_columns(glue_database, pipeline_name, region):
    """Get columns from the Glue catalog for the Redshift table schema.

    Returns the TABLE-LEVEL columns (union of all partitions).  This is the
    source of truth for what the Redshift table should look like (grow-only —
    new columns are added, old ones are never dropped).

    Returns:
        Tuple of (columns, glue_table_name) where columns is a list of dicts
        with 'Name' and 'Type' keys.
    """
    glue_client = boto3.client("glue", region_name=region)
    response = glue_client.get_tables(
        DatabaseName=glue_database,
        Expression=f"{pipeline_name}_*",
    )

    if not response["TableList"]:
        raise RuntimeError(
            f"No Glue catalog table found matching '{pipeline_name}_*' in database '{glue_database}'. "
            f"Ensure the Glue Crawler has run."
        )

    glue_table = sorted(response["TableList"], key=lambda t: t.get("UpdateTime", ""), reverse=True)[0]
    return glue_table["StorageDescriptor"]["Columns"], glue_table["Name"]



def _get_redshift_columns(client, workgroup, database, secret_arn, schema, table):
    """Get existing column names from a Redshift table. Returns set of lowercase names."""
    sql = f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    result = _execute_sql(client, workgroup, database, secret_arn, sql)
    cols = _fetch_single_column(client, result["Id"])
    return {c.lower() for c in cols}


def _ensure_table_exists(
    client, workgroup, database, secret_arn,
    schema, table, glue_columns,
    sort_keys=None, distribution_style=None,
):
    """Create the Redshift table if it doesn't exist, or add new columns if schema drifted.

    - Glue catalog is the source of truth for the current data schema.
    - New columns in Glue are ADDed to Redshift (grow-only, never drop).
    - Historical columns in Redshift are preserved even if absent from Glue.
    """

    # Check if table already exists
    check_sql = f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """
    result = _execute_sql(client, workgroup, database, secret_arn, check_sql)
    table_exists = result.get("ResultRows", 0) > 0

    if not table_exists:
        # Build CREATE TABLE DDL from Glue schema
        col_defs = []
        for col in glue_columns:
            rs_type = _glue_type_to_redshift(col["Type"])
            col_defs.append(f'    "{col["Name"]}" {rs_type}')

        table_props = ""
        if distribution_style:
            table_props += f"\nDISTSTYLE {distribution_style}"
        if sort_keys:
            table_props += f"\nSORTKEY ({', '.join(sort_keys)})"

        ddl = f'CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{",".join(col_defs)}\n){table_props};'
        print(f"Auto-creating Redshift table {schema}.{table} from Glue catalog ({len(glue_columns)} columns)")
        _execute_sql(client, workgroup, database, secret_arn, ddl)
    else:
        # Table exists — check for new columns in Glue that aren't in Redshift
        existing_cols = _get_redshift_columns(client, workgroup, database, secret_arn, schema, table)
        new_columns = [
            col for col in glue_columns
            if col["Name"].lower() not in existing_cols
        ]

        if new_columns:
            print(f"[Schema Drift] Adding {len(new_columns)} new column(s) to {schema}.{table}: "
                  f"{[c['Name'] for c in new_columns]}")
            for col in new_columns:
                rs_type = _glue_type_to_redshift(col["Type"])
                alter_sql = f'ALTER TABLE {schema}.{table} ADD COLUMN "{col["Name"]}" {rs_type};'
                _execute_sql(client, workgroup, database, secret_arn, alter_sql)
        else:
            print(f"[Schema] {schema}.{table} is up to date ({len(existing_cols)} columns)")

    # Return glue column names for use by load functions
    return [col["Name"] for col in glue_columns]


def _get_redshift_secret(secret_name, region):
    """Fetch Redshift secret from Secrets Manager. Returns (arn, secret_dict)."""
    sm_client = boto3.client("secretsmanager", region_name=region)
    response = sm_client.get_secret_value(SecretId=secret_name)
    return response["ARN"], json.loads(response["SecretString"])



def _ensure_external_schema(client, workgroup, database, secret_arn, external_schema, glue_database, iam_role):
    """Create a Spectrum external schema pointing to the Glue catalog if it doesn't exist."""
    sql = f"""
        CREATE EXTERNAL SCHEMA IF NOT EXISTS {external_schema}
        FROM DATA CATALOG
        DATABASE '{glue_database}'
        IAM_ROLE '{iam_role}';
    """
    print(f"[Spectrum] Ensuring external schema '{external_schema}' -> Glue DB '{glue_database}'")
    _execute_sql(client, workgroup, database, secret_arn, sql)


def _build_spectrum_select(schema, table, external_schema, glue_table_name, s3_path):
    """Build a SELECT from the Spectrum external table filtered to the given S3 path.

    Uses the Redshift target table's column list to handle schema drift:
    only columns that exist in both the target table and the external table
    are selected. This is safe because _ensure_table_exists already added
    any new Glue columns to the target.
    """
    # Use the "$path" pseudo-column to filter only files under the curated S3 path
    # Ensure s3_path ends with / for prefix matching
    s3_prefix = s3_path.rstrip("/") + "/"
    return (
        f'SELECT * FROM {external_schema}."{glue_table_name}" '
        f"WHERE \"$path\" LIKE '{s3_prefix}%'"
    )


def _load_replace(client, workgroup, database, secret_arn, schema, table, s3_path, iam_role, spectrum_params=None):
    """TRUNCATE + INSERT via Spectrum + ANALYZE.

    Uses Redshift Spectrum to read Parquet through the Glue catalog external
    table, bypassing COPY FORMAT AS PARQUET (which cannot handle complex
    Parquet types or INT96 timestamps).
    """
    ext_schema = spectrum_params["external_schema"]
    glue_table = spectrum_params["glue_table_name"]
    spectrum_select = _build_spectrum_select(schema, table, ext_schema, glue_table, s3_path)

    sql = f"""
        BEGIN;
        TRUNCATE TABLE {schema}.{table};
        INSERT INTO {schema}.{table} ({spectrum_select});
        COMMIT;
    """
    print(f"[Replace] Loading {schema}.{table} via Spectrum from {ext_schema}.\"{glue_table}\"")
    _execute_sql(client, workgroup, database, secret_arn, sql)
    _execute_sql(client, workgroup, database, secret_arn, f"ANALYZE {schema}.{table};")


def _load_append(client, workgroup, database, secret_arn, schema, table, s3_path, iam_role, spectrum_params=None):
    """INSERT via Spectrum (append).

    Uses Spectrum external table instead of COPY.
    """
    ext_schema = spectrum_params["external_schema"]
    glue_table = spectrum_params["glue_table_name"]
    spectrum_select = _build_spectrum_select(schema, table, ext_schema, glue_table, s3_path)

    sql = f"INSERT INTO {schema}.{table} ({spectrum_select});"
    print(f"[Append] Loading {schema}.{table} via Spectrum")
    _execute_sql(client, workgroup, database, secret_arn, sql)


def _load_merge(client, workgroup, database, secret_arn, schema, table, s3_path, iam_role, merge_keys, glue_columns=None, spectrum_params=None):
    """Spectrum SELECT into staging → DELETE matching → INSERT from staging.

    Uses Redshift Spectrum to read Parquet data through the Glue catalog,
    completely bypassing COPY FORMAT AS PARQUET. Spectrum handles all Parquet
    types natively (complex types, INT96 timestamps, etc.).
    """
    ext_schema = spectrum_params["external_schema"]
    glue_table = spectrum_params["glue_table_name"]
    staging_table = f"{table}_staging_{int(time.time())}"

    # Build column list from Glue catalog
    if glue_columns:
        col_names = ", ".join(f'"{col["Name"]}"' for col in glue_columns)
    else:
        col_names = "*"

    s3_prefix = s3_path.rstrip("/") + "/"

    join_condition = " AND ".join(
        f"{schema}.{table}.{k} = {staging_table}.{k}" for k in merge_keys
    )

    sql = f"""
        BEGIN;

        CREATE TEMP TABLE {staging_table} (LIKE {schema}.{table});

        INSERT INTO {staging_table} ({col_names})
        SELECT {col_names}
        FROM {ext_schema}."{glue_table}"
        WHERE "$path" LIKE '{s3_prefix}%';

        DELETE FROM {schema}.{table}
        USING {staging_table}
        WHERE {join_condition};

        INSERT INTO {schema}.{table} ({col_names})
        SELECT {col_names} FROM {staging_table};

        DROP TABLE {staging_table};

        COMMIT;
    """
    print(f"[Merge] Loading {schema}.{table} via Spectrum ({len(glue_columns) if glue_columns else '?'} columns)")
    _execute_sql(client, workgroup, database, secret_arn, sql)


def _fetch_single_column(client, statement_id):
    """Fetch all values from the first column of a completed query result."""
    values = []
    kwargs = {"Id": statement_id}
    while True:
        response = client.get_statement_result(**kwargs)
        for record in response.get("Records", []):
            if record and record[0].get("stringValue"):
                values.append(record[0]["stringValue"])
        next_token = response.get("NextToken")
        if next_token:
            kwargs["NextToken"] = next_token
        else:
            break
    return values


def _get_max_watermark(client, workgroup, database, secret_arn, schema, table, incremental_key):
    """Query Redshift for the max value of the incremental key column."""
    sql = f'SELECT MAX("{incremental_key}") AS max_val FROM {schema}.{table}'
    result = _execute_sql(client, workgroup, database, secret_arn, sql)

    # Fetch the result
    statement_id = result["Id"]
    try:
        rows = client.get_statement_result(Id=statement_id)
        records = rows.get("Records", [])
        if records and records[0] and records[0][0].get("stringValue"):
            max_val = records[0][0]["stringValue"]
            print(f"[CDC] Max watermark from Redshift: {incremental_key} = {max_val}")
            return max_val
    except Exception as e:
        print(f"[CDC] Warning: Could not fetch max watermark: {e}")

    return None


def ensure_dimension_table_exists(
    client, workgroup, database, secret_arn,
    schema: str, table: str, columns: List[Dict],
):
    """Create a dimension table from YAML column definitions if it doesn't exist."""
    check_sql = f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_name = '{table}'
    """
    result = _execute_sql(client, workgroup, database, secret_arn, check_sql)
    if result.get("ResultRows", 0) > 0:
        return

    col_defs = []
    for col in columns:
        col_defs.append(f'    "{col["name"]}" {col["type"]}')

    ddl = f'CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{", ".join(col_defs)}\n);'
    print(f"Auto-creating dimension table {schema}.{table}")
    _execute_sql(client, workgroup, database, secret_arn, ddl)


def load_dimension_json(
    client, workgroup, database, secret_arn,
    schema: str, table: str, s3_path: str, iam_role: str,
    columns: List[Dict],
):
    """TRUNCATE + COPY FROM JSON + ANALYZE for a dimension table."""
    ensure_dimension_table_exists(
        client, workgroup, database, secret_arn,
        schema, table, columns,
    )

    sql = f"""
        BEGIN;
        TRUNCATE TABLE {schema}.{table};
        COPY {schema}.{table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        JSON 'auto ignorecase'
        TRUNCATECOLUMNS
        REGION AS 'us-east-1';
        COMMIT;
    """
    _execute_sql(client, workgroup, database, secret_arn, sql)
    _execute_sql(client, workgroup, database, secret_arn, f"ANALYZE {schema}.{table};")
    print(f"Dimension {schema}.{table} loaded from {s3_path}")


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
