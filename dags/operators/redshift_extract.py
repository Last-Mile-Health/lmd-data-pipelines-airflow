"""
Redshift view extractor.

Extracts data from Redshift Serverless views via the Data API.
Returns rows as a list of dicts for downstream transform/load steps.

Reuses _get_redshift_secret and _execute_sql from redshift_load.py.
"""
import boto3
from typing import Dict, Any, List

from operators.redshift_load import _get_redshift_secret, _execute_sql


def extract_view(
    config: Dict,
    env_config: Dict,
    view_cfg: Dict,
) -> List[Dict[str, Any]]:
    """
    Extract all rows from a Redshift view.

    Args:
        config: Pipeline YAML config
        env_config: Environment config
        view_cfg: View config dict with keys: database, schema, view

    Returns:
        List of dicts, one per row
    """
    # Per-view database overrides the top-level redshift.database
    database = (
        view_cfg.get("database")
        or config.get("redshift", {}).get("database")
        or env_config["redshift_database"]
    )
    schema = view_cfg.get("schema", "public")
    view = view_cfg["view"]

    secret_arn, secret_values = _get_redshift_secret(
        env_config["redshift_secret_name"], env_config["aws_region"]
    )
    workgroup = secret_values["workgroupName"]

    client = boto3.client("redshift-data", region_name=env_config["aws_region"])

    qualified_view = f"{schema}.{view}"
    sql = f"SELECT * FROM {qualified_view}"
    print(f"[OKR Extract] Running on database '{database}': {sql}")

    result = _execute_sql(client, workgroup, database, secret_arn, sql)
    statement_id = result["Id"]

    # Paginate through results
    rows = []
    col_names = None
    kwargs = {"Id": statement_id}

    while True:
        response = client.get_statement_result(**kwargs)

        # Build column names from metadata on first page
        if col_names is None:
            col_meta = response["ColumnMetadata"]
            col_names = [col["name"] for col in col_meta]

        # Convert Redshift Data API records to dicts
        for record in response.get("Records", []):
            row = {}
            for i, field in enumerate(record):
                # Data API returns typed values: stringValue, longValue, etc.
                value = None
                for type_key in ("stringValue", "longValue", "doubleValue", "booleanValue"):
                    if type_key in field:
                        value = field[type_key]
                        break
                if "isNull" in field and field["isNull"]:
                    value = None
                row[col_names[i]] = value
            rows.append(row)

        # Check for more pages
        next_token = response.get("NextToken")
        if next_token:
            kwargs["NextToken"] = next_token
        else:
            break

    print(f"[OKR Extract] Extracted {len(rows)} rows from {database}.{qualified_view}")
    return rows
