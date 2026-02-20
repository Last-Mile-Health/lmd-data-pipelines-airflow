"""
RDS PostgreSQL loader.

Writes rows (list of dicts) into an RDS PostgreSQL table.
Supports write_mode 'replace' (TRUNCATE + INSERT).

Fetches username/password from Secrets Manager; host, port, dbname
come from the pipeline YAML target config.
"""
import json
from typing import Dict, Any, List

import boto3
import psycopg2
from psycopg2.extras import execute_values


def _get_rds_secret(secret_name: str, region: str) -> Dict[str, str]:
    """Fetch RDS credentials (username, password) from Secrets Manager."""
    sm_client = boto3.client("secretsmanager", region_name=region)
    response = sm_client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def _get_connection(target_cfg: Dict, secret: Dict[str, str]):
    """Create a psycopg2 connection from target config + secret credentials."""
    return psycopg2.connect(
        host=target_cfg["writer_endpoint"],
        port=int(target_cfg.get("port", 5432)),
        user=secret["username"],
        password=secret["password"],
        dbname=target_cfg.get("dbname", "postgres"),
    )


def _ensure_table(cursor, schema: str, table: str, columns: List[str]):
    """Create the target table if it doesn't exist (all TEXT columns)."""
    col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
    cursor.execute(
        f'CREATE TABLE IF NOT EXISTS {schema}."{table}" ({col_defs});'
    )


def load_to_rds(
    rows: List[Dict[str, Any]],
    target_cfg: Dict,
    target_table: str,
    region: str,
) -> Dict[str, Any]:
    """
    Load rows into an RDS PostgreSQL table.

    Args:
        rows: List of dicts to insert
        target_cfg: Target config from YAML (writer_endpoint, port, dbname,
                    secret_name, schema, write_mode)
        target_table: Target table name
        region: AWS region

    Returns:
        Dict with status and row count
    """
    schema = target_cfg.get("schema", "public")
    write_mode = target_cfg.get("write_mode", "replace")

    if not rows:
        print(f"[OKR Load] No rows to load into {schema}.{target_table}")
        return {"status": "skipped", "rows_loaded": 0}

    secret = _get_rds_secret(target_cfg["secret_name"], region)
    conn = _get_connection(target_cfg, secret)

    try:
        columns = list(rows[0].keys())
        col_list = ", ".join(f'"{c}"' for c in columns)

        with conn.cursor() as cur:
            # Auto-create table if missing
            _ensure_table(cur, schema, target_table, columns)

            if write_mode == "replace":
                cur.execute(f'TRUNCATE TABLE {schema}."{target_table}";')

            # Bulk insert
            insert_sql = f'INSERT INTO {schema}."{target_table}" ({col_list}) VALUES %s'
            values = [tuple(row.get(c) for c in columns) for row in rows]
            execute_values(cur, insert_sql, values, page_size=1000)

        conn.commit()
        print(f"[OKR Load] Loaded {len(rows)} rows into {schema}.{target_table}")

        return {"status": "loaded", "rows_loaded": len(rows), "table": f"{schema}.{target_table}"}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
