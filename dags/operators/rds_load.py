"""
RDS PostgreSQL loader.

Writes rows (list of dicts) into an RDS PostgreSQL table.
Supports write_mode 'replace' (TRUNCATE + INSERT) and 'okr_monthly_update'
(UPDATE existing MonthlyUpdate rows by indicator lookup).

Fetches username/password from Secrets Manager; host, port, dbname
come from the pipeline YAML target config or the secret itself.
"""
import json
import uuid
import datetime
from typing import Dict, Any, List, Optional

import boto3
import psycopg2
from psycopg2.extras import execute_values


def _get_rds_secret(secret_name: str, region: str) -> Dict[str, str]:
    """Fetch RDS credentials (username, password) from Secrets Manager."""
    sm_client = boto3.client("secretsmanager", region_name=region)
    response = sm_client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def _get_connection(target_cfg: Dict, secret: Dict[str, str]):
    """Create a psycopg2 connection from target config + secret credentials.

    Host resolution priority:
      1. target_cfg['writer_endpoint']  — explicit YAML config
      2. secret['writer']               — Aurora cluster writer endpoint key
      3. secret['host']                 — standard RDS host key
    Port and dbname fall back to secret values when not set in target_cfg.
    """
    host = (
        target_cfg.get("writer_endpoint")
        or secret.get("writer")
        or secret.get("host")
    )
    if not host:
        raise ValueError(
            "RDS host not found. Expected one of: target_cfg.writer_endpoint, "
            "secret.writer, or secret.host"
        )
    port = int(target_cfg.get("port") or secret.get("port", 5432))
    dbname = target_cfg.get("dbname") or secret.get("dbname", "postgres")
    return psycopg2.connect(
        host=host,
        port=port,
        user=secret["username"],
        password=secret["password"],
        dbname=dbname,
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


def _compute_okr_status(value: Optional[float], target_value: Optional[float]) -> str:
    """Derive OkrStatus from value as a percentage of targetValue.

    Thresholds (% of target achieved):
        >= 100%  → achieved
        >=  75%  → on_track
        >=  50%  → delayed
        >=  25%  → at_risk
        <   25%  → okr_under_review
    """
    if value is None or not target_value:
        return "on_track"
    pct_of_target = (value / target_value) * 100
    if pct_of_target >= 100:
        return "achieved"
    if pct_of_target >= 75:
        return "on_track"
    if pct_of_target >= 50:
        return "delayed"
    if pct_of_target >= 25:
        return "at_risk"
    return "okr_under_review"


def update_okr_monthly_update(
    rows: List[Dict[str, Any]],
    target_cfg: Dict,
    region: str,
) -> Dict[str, Any]:
    """
    Upsert MonthlyUpdate rows in RDS okr_data.

    Only processes rows where managed_county == 1 (LMH-managed areas).
    Non-managed rows are skipped — there is one OKR per indicator and
    managed data takes precedence.

    For each managed pivot row:
      1. Look up "Okr".id WHERE "okrId" = indicator_id  (e.g. '2.1')
      2. UPSERT into "MonthlyUpdate":
           - INSERT when no row exists for (okrId, month, year), seeding
             narrative='' and status derived from value vs targetValue.
           - ON CONFLICT update pipeline-owned columns:
             numerator, denominatorOverride, value, status, updatedAt.
             narrative/risks/mitigations entered by users are preserved.

    Args:
        rows:       Pivoted indicator rows from _pivot_to_indicator_rows()
        target_cfg: Target config from YAML (secret_name, dbname, schema,
                    okr_lookup_field)
        region:     AWS region for Secrets Manager

    Returns:
        Dict with status, rows_upserted, rows_skipped
    """
    schema = target_cfg.get("schema", "public")
    okr_lookup_field = target_cfg.get("okr_lookup_field", "okrId")

    secret = _get_rds_secret(target_cfg["secret_name"], region)
    conn = _get_connection(target_cfg, secret)

    upserted = 0
    skipped  = 0

    try:
        with conn.cursor() as cur:
            # Cache indicator_id → (Okr.id cuid, targetValue)
            okr_cache: Dict[str, Optional[tuple]] = {}

            for row in rows:
                indicator_id = row["indicator_id"]
                cy_month     = int(row["cy_month"])
                cy_year      = int(row["cy_year"])
                managed      = int(row.get("managed_county") or 0)

                # Only use managed county data
                if managed != 1:
                    skipped += 1
                    continue

                numerator   = row.get("numerator")
                denominator = row.get("denominator")

                # Calculate value as percentage (0–100), null-safe
                if numerator is not None and denominator:
                    value = round(float(numerator) / float(denominator) * 100, 2)
                else:
                    value = None

                # Resolve Okr primary key + targetValue (cached per indicator)
                if indicator_id not in okr_cache:
                    cur.execute(
                        f'SELECT id, "targetValue" FROM {schema}."Okr" '
                        f'WHERE "{okr_lookup_field}" = %s '
                        f'LIMIT 1',
                        (indicator_id,),
                    )
                    result = cur.fetchone()
                    if result:
                        okr_cache[indicator_id] = (result[0], result[1])
                    else:
                        print(
                            f"[OKR Upsert] No Okr found for "
                            f"{okr_lookup_field}='{indicator_id}' — skipping"
                        )
                        okr_cache[indicator_id] = None

                okr_entry = okr_cache[indicator_id]
                if okr_entry is None:
                    skipped += 1
                    continue

                okr_id, target_value   = okr_entry
                status                 = _compute_okr_status(value, target_value)
                pipeline_email         = "lmdadmin@lastmilehealth.org"
                now                    = datetime.datetime.utcnow()
                row_id                 = uuid.uuid4().hex

                # UPSERT — insert with computed status; on conflict update numeric
                # columns and status. narrative/risks/mitigations entered by the
                # user in the app are preserved on conflict.
                cur.execute(
                    f"""
                    INSERT INTO {schema}."MonthlyUpdate"
                        (id, numerator, "denominatorOverride", value,
                         narrative, status, risks, mitigations,
                         month, year, "okrId",
                         "createdByEmail", "updatedByEmail",
                         "createdAt", "updatedAt")
                    VALUES (%s, %s, %s, %s, '', %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ("okrId", month, year) DO UPDATE
                        SET numerator             = EXCLUDED.numerator,
                            "denominatorOverride" = EXCLUDED."denominatorOverride",
                            value                 = EXCLUDED.value,
                            status                = EXCLUDED.status,
                            "updatedByEmail"      = EXCLUDED."updatedByEmail",
                            "updatedAt"           = EXCLUDED."updatedAt"
                    """,
                    (
                        row_id,
                        numerator,
                        denominator,
                        value,
                        status,          # computed from value vs targetValue
                        [],              # risks  (empty text[])
                        [],              # mitigations (empty text[])
                        cy_month,
                        cy_year,
                        okr_id,
                        pipeline_email,  # createdByEmail
                        pipeline_email,  # updatedByEmail
                        now,             # createdAt
                        now,             # updatedAt
                    ),
                )
                upserted += 1
                print(
                    f"[OKR Upsert] indicator={indicator_id} {cy_year}-{cy_month:02d} "
                    f"numerator={numerator} denominator={denominator} "
                    f"value={value} target={target_value} status={status}"
                )

        conn.commit()
        print(
            f"[OKR Upsert] Done — {upserted} upserted, {skipped} skipped "
            f"in {schema}.MonthlyUpdate"
        )
        return {"status": "upserted", "rows_upserted": upserted, "rows_skipped": skipped}

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
