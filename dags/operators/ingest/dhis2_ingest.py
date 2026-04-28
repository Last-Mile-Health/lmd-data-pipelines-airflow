"""
DHIS2 API ingestor.

Pulls analytics/data values from a DHIS2 instance and writes raw JSON to Raw (S3).
Supports multiple DHIS2 endpoints: analytics, dataValueSets, events, trackedEntityInstances.

Pagination strategy is endpoint-aware:
    /analytics            → uses pager (paging=true)
    /dataValueSets        → single response (no pagination)
    /events               → totalPages=true + pager
    /trackedEntityInstances → totalPages=true + pager
"""
import json
import logging
import time
import requests
import boto3
from typing import Dict, List, Tuple
from dateutil.parser import isoparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.s3_helpers import build_raw_path, write_json_to_s3

log = logging.getLogger(__name__)

# Hard cap to prevent infinite pagination loops on misbehaving endpoints
MAX_PAGES_SAFETY_CAP = 10_000

# Body-read retry config for ChunkedEncodingError / mid-stream drops, which
# urllib3.Retry cannot catch (response headers already returned 200).
BODY_READ_RETRIES = 3
BODY_READ_BACKOFF = 2.0  # seconds, exponential

# /dataValueSets returns the entire window in one response body. Large windows
# overwhelm the server mid-stream — batch into this many periods per request.
DEFAULT_PERIOD_BATCH_SIZE = 1


def build_retrying_session(total: int = 3, backoff_factor: float = 1.5) -> requests.Session:
    """Session with automatic retry on transient network/server failures.

    Catches DNS hiccups (via connection retries), connection resets, and 5xx
    responses without burning Airflow task retries.
    """
    session = requests.Session()
    retry = Retry(
        total=total,
        connect=total,           # DNS / connect failures
        read=total,
        status=total,
        backoff_factor=backoff_factor,
        status_forcelist=[502, 503, 504, 429],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _get_with_body_retry(session, url, headers, auth, params, timeout: int = 300):
    """GET that retries body-read failures (ChunkedEncodingError, ConnectionError).

    urllib3.Retry only covers handshake/status failures — once the server returns
    200 and starts streaming, a mid-body drop raises ChunkedEncodingError on the
    .content / .json() read and isn't retried. This wrapper closes that gap.
    """
    last_exc = None
    for attempt in range(1, BODY_READ_RETRIES + 1):
        try:
            resp = session.get(url, headers=headers, auth=auth, params=params, timeout=timeout)
            resp.raise_for_status()
            # Force the body read here so chunked drops surface inside the retry loop.
            _ = resp.content
            return resp
        except (requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError) as e:
            last_exc = e
            if attempt == BODY_READ_RETRIES:
                break
            sleep_for = BODY_READ_BACKOFF ** attempt
            log.warning(
                f"[dhis2] body-read failure ({type(e).__name__}) on attempt {attempt}/"
                f"{BODY_READ_RETRIES}; retrying in {sleep_for:.1f}s — {e}"
            )
            time.sleep(sleep_for)
    raise last_exc


def _endpoint_pagination_mode(endpoint: str) -> str:
    """Classify a DHIS2 endpoint by how it paginates."""
    ep = endpoint.lower()
    if "analytics" in ep:
        return "pager"
    if "datavaluesets" in ep:
        return "none"
    if "events" in ep or "trackedentityinstances" in ep or "trackedentities" in ep:
        return "pager_total"
    return "pager"  # safe default


def _extract_records(data: dict) -> List:
    """Pull the record list out of a DHIS2 response, regardless of shape."""
    if "rows" in data:
        headers_list = data.get("headers", [])
        header_names = [h.get("name", h.get("column", f"col_{i}")) for i, h in enumerate(headers_list)]
        return [dict(zip(header_names, row)) for row in data["rows"]]
    for key in ("dataValues", "trackedEntityInstances", "events", "instances"):
        if key in data:
            return data[key]
    if isinstance(data, list):
        return data
    return [data]


def run(config: Dict, env_config: Dict, load_params: Dict) -> Dict:
    """Ingest data from DHIS2 API → Raw S3."""
    source_cfg = config["source"]["config"]
    secret_name = config["source"].get("secret_name")
    secrets = _get_secrets(secret_name) if secret_name else {}

    base_url = source_cfg.get("base_url", secrets.get("DHIS2_BASE_URL", "")).rstrip("/")
    endpoint = source_cfg.get("endpoint", "/api/analytics")
    auth_type = source_cfg.get("auth_type", "basic")

    auth = None
    headers = {"Accept": "application/json"}
    if auth_type == "basic":
        auth = (secrets.get("DHIS2_USERNAME", ""), secrets.get("DHIS2_PASSWORD", ""))
    elif auth_type == "token":
        # DHIS2 PATs use "ApiToken", not "Bearer".
        headers["Authorization"] = f"ApiToken {secrets.get('DHIS2_TOKEN', '')}"

    # Build query params (YAML defaults, overridden by runtime conf)
    params = dict(source_cfg.get("params", {}))
    runtime_params = load_params.get("source_params", {}) or {}
    if runtime_params:
        params.update(runtime_params)

    dimensions = source_cfg.get("dimensions", [])
    if dimensions:
        for dim in dimensions:
            params.setdefault("dimension", [])
            if isinstance(params["dimension"], list):
                params["dimension"].append(dim)

    # CDC strategy-aware param handling
    cdc_strategy = load_params.get("cdc_strategy", load_params.get("mode", "full"))
    user_set_period = "period" in runtime_params

    if cdc_strategy == "incremental":
        if not user_set_period:
            params.pop("period", None)
        if load_params.get("start_after"):
            params["lastUpdated"] = load_params["start_after"]

    elif cdc_strategy == "rolling_window":
        if not user_set_period:
            params.pop("period", None)
            period_list = load_params.get("period_list", [])
            if period_list:
                params["period"] = period_list

    elif cdc_strategy == "full":
        # 'full' must specify a period explicitly (via YAML or runtime conf) —
        # otherwise the query is unbounded and may pull years of data.
        if not params.get("period"):
            raise ValueError(
                "cdc_strategy=full requires an explicit 'period' (set in YAML "
                "source.config.params.period or pass via dag_run.conf.source_params.period)"
            )

    url = f"{base_url}{endpoint}"
    pagination_mode = _endpoint_pagination_mode(endpoint)
    log.info(f"[dhis2] GET {url} | pagination={pagination_mode}")

    session = build_retrying_session()

    # When /dataValueSets (mode=none) is asked for many periods in one shot,
    # the server often drops the connection mid-stream. Batch the periods into
    # smaller requests and concatenate.
    ingestion_cfg = config.get("ingestion", {}) or {}
    batch_size = int(ingestion_cfg.get("period_batch_size", DEFAULT_PERIOD_BATCH_SIZE))
    period_value = params.get("period")
    period_list = period_value if isinstance(period_value, list) else None

    if pagination_mode == "none" and period_list and batch_size > 0 and len(period_list) > batch_size:
        all_data: List = []
        total_batches = (len(period_list) + batch_size - 1) // batch_size
        for i in range(0, len(period_list), batch_size):
            chunk = period_list[i:i + batch_size]
            batch_idx = i // batch_size + 1
            log.info(f"[dhis2] batch {batch_idx}/{total_batches} periods={chunk}")
            batch_params = dict(params)
            batch_params["period"] = chunk
            batch_records = _fetch_all(
                session, url, headers, auth, batch_params,
                pagination_mode, source_cfg.get("page_size", 1000),
            )
            log.info(f"[dhis2] batch {batch_idx}/{total_batches} → {len(batch_records):,} records")
            all_data.extend(batch_records)
    else:
        all_data = _fetch_all(session, url, headers, auth, params, pagination_mode, source_cfg.get("page_size", 1000))

    # Compute max watermark — parse to datetime so comparison is correct,
    # then re-emit as ISO string for downstream storage.
    incremental_key = load_params.get("incremental_key")
    max_watermark = None
    skipped = 0
    if incremental_key and all_data:
        parsed = []
        for r in all_data:
            v = r.get(incremental_key)
            if not v:
                skipped += 1
                continue
            try:
                parsed.append(isoparse(str(v)))
            except (ValueError, TypeError):
                skipped += 1
        if parsed:
            max_watermark = max(parsed).isoformat()
        if skipped:
            log.warning(f"[dhis2] {skipped} records missing or unparseable {incremental_key}")

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
        data=all_data,
    )

    return {
        "s3_path": s3_path,
        "s3_key": s3_key,
        "record_count": len(all_data),
        "max_watermark": max_watermark,
        "source_type": "dhis2",
    }


def _fetch_all(session, url, headers, auth, params, mode: str, page_size: int) -> List:
    """Endpoint-aware fetch. Returns concatenated records across pages."""
    all_data: List = []

    if mode == "none":
        # /dataValueSets and similar — single shot, no pagination params.
        params.pop("page", None)
        params.pop("pageSize", None)
        params.pop("paging", None)
        resp = _get_with_body_retry(session, url, headers, auth, params)
        all_data.extend(_extract_records(resp.json()))
        return all_data

    # Both 'pager' and 'pager_total' use page/pageSize; pager_total adds totalPages=true
    params["pageSize"] = page_size
    params["page"] = 1
    if mode == "pager_total":
        params["totalPages"] = "true"

    for page_num in range(1, MAX_PAGES_SAFETY_CAP + 1):
        params["page"] = page_num
        resp = _get_with_body_retry(session, url, headers, auth, params)
        data = resp.json()

        records = _extract_records(data)
        all_data.extend(records)

        pager = data.get("pager") or {}
        total_pages = pager.get("pageCount") or pager.get("total")
        if total_pages:
            if page_num >= int(total_pages):
                break
        else:
            # No pager in response — assume single page rather than loop forever.
            if not records:
                break
            # Defensive: if records came back but no pager hint, stop after first page
            # for endpoints that don't honor paging at all.
            log.warning(f"[dhis2] No pager in response — exiting after page {page_num}")
            break
    else:
        log.error(f"[dhis2] Hit MAX_PAGES_SAFETY_CAP={MAX_PAGES_SAFETY_CAP} — aborting pagination")

    return all_data


def _get_secrets(secret_name: str) -> Dict:
    """Fetch secrets from AWS Secrets Manager."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])
