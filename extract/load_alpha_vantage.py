"""Extract Alpha Vantage prices and company overviews into BigQuery.

Landing rules for this script, which exist so that dbt owns all semantics:

  * Python does STRUCTURAL work only -- turning the API's date-keyed objects
    into rows, and sanitising key names that BigQuery would reject.
  * Every value lands as a STRING. No casting, no renaming beyond what is
    required for a column name to be legal, no business logic.
  * Raw tables are APPEND-ONLY. Re-running the script is safe; duplicates are
    expected and are resolved in the dbt staging layer, which keeps the newest
    row per (ticker, trade_date) by _extracted_at.

Usage:
    python extract/load_alpha_vantage.py --daily
    python extract/load_alpha_vantage.py --weekly --overview
    python extract/load_alpha_vantage.py --all --dry-run
"""

import argparse
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

# .env sits at the repo root and this file is one level down in extract/.
# Deriving the path from __file__ means the script works no matter which
# directory you happen to run it from.
REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    raise RuntimeError(
        "ALPHAVANTAGE_API_KEY is not set. Add it to .env at the repo root."
    )

GCP_KEYFILE = os.environ.get("GCP_KEYFILE")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_RAW_DATASET = os.environ.get("BQ_RAW_DATASET", "raw")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

BASE_URL = "https://www.alphavantage.co/query"
REQUEST_TIMEOUT_SECONDS = 30

# Alpha Vantage answers HTTP 200 even when it is refusing to give you data.
# The real status arrives in one of these top-level keys instead.
ERROR_KEYS = ("Error Message", "Note", "Information")

# Eight symbols keeps a full --all run at 24 calls, just inside the free
# tier's 25/day. Add an ETF such as SPY to see the orphaned-dimension case:
# it has price data but no OVERVIEW record.
TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "JPM", "XOM", "JNJ"]

# function -> (top-level key holding the series, destination table)
PRICE_SERIES = {
    "TIME_SERIES_DAILY": ("Time Series (Daily)", "raw_daily_prices"),
    "TIME_SERIES_WEEKLY": ("Weekly Time Series", "raw_weekly_prices"),
}
OVERVIEW_TABLE = "raw_company_overview"

# Reused across calls so the TLS handshake happens once, not once per ticker.
_SESSION = requests.Session()

# The free tier enforces roughly 1 request per second on top of the 25/day
# cap. Throttling inside fetch means no caller can forget to do it.
MIN_SECONDS_BETWEEN_CALLS = 1.5
_last_call_at = 0.0

_bq_client: bigquery.Client | None = None

log = logging.getLogger("alpha_vantage")


def _throttle() -> None:
    """Block until MIN_SECONDS_BETWEEN_CALLS has passed since the last call."""
    global _last_call_at
    elapsed = time.monotonic() - _last_call_at
    if elapsed < MIN_SECONDS_BETWEEN_CALLS:
        time.sleep(MIN_SECONDS_BETWEEN_CALLS - elapsed)
    _last_call_at = time.monotonic()


def fetch(
    symbol: str,
    function: str,
    expect_key: str | None = None,
    outputsize: str = "compact",
) -> dict:
    """Call one Alpha Vantage endpoint and return the parsed JSON.

    Args:
        symbol: Ticker symbol, e.g. "IBM".
        function: Alpha Vantage function name, e.g. "TIME_SERIES_DAILY".
        expect_key: Optional top-level key the caller requires to be present.
            Lets each caller assert the shape it needs without fetch itself
            having to know which endpoint returns what.
        outputsize: "compact" returns the last 100 sessions. "full" returns
            20+ years but is a PREMIUM feature on TIME_SERIES_DAILY as of
            2026-08 -- it fails with an Information message on the free tier.

    Returns:
        The decoded JSON body.

    Raises:
        RuntimeError: The API returned an error message, a throttle notice,
            a non-JSON body, an empty body, or a payload missing expect_key.
        requests.HTTPError: Genuine transport-level failure (4xx/5xx).
    """
    params = {
        "function": function,
        "symbol": symbol,
        "apikey": API_KEY,
        # Harmlessly ignored by endpoints that do not paginate, e.g. OVERVIEW.
        "outputsize": outputsize,
    }

    _throttle()
    response = _SESSION.get(
        BASE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS
    )
    response.raise_for_status()

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"{function} for {symbol}: response was not JSON. "
            f"First 200 chars: {response.text[:200]!r}"
        ) from exc

    for key in ERROR_KEYS:
        if key in payload:
            raise RuntimeError(f"{function} for {symbol}: {payload[key]}")

    if not payload:
        raise RuntimeError(
            f"{function} for {symbol}: empty response. Common for ETFs and "
            f"other symbols that have no OVERVIEW record."
        )

    if expect_key and expect_key not in payload:
        raise RuntimeError(
            f"{function} for {symbol}: expected key {expect_key!r} was not "
            f"found. Top-level keys returned: {sorted(payload)}"
        )

    return payload


def _safe_column_name(key: str) -> str:
    """Coerce an API field name into a legal BigQuery column name.

    BigQuery columns must match [A-Za-z_][A-Za-z_0-9]*, which rules out the
    price fields ("1. open") and some overview fields ("52WeekHigh"). The
    transformation is deliberately mechanical -- strip a leading "N. " index,
    replace illegal characters, prefix a leading digit -- so that no meaning
    is assigned here. Semantic renaming happens in dbt staging.

        "1. open"     -> "open"
        "52WeekHigh"  -> "_52WeekHigh"
        "Market Cap"  -> "Market_Cap"
    """
    cleaned = re.sub(r"^\d+\.\s*", "", key.strip())
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", cleaned)
    if not cleaned:
        raise ValueError(f"Field name {key!r} sanitises to an empty string")
    if cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned


def flatten_prices(symbol: str, payload: dict, series_key: str) -> list[dict]:
    """Turn a date-keyed price object into one row per trading session.

    Alpha Vantage returns the series as an object keyed by date rather than an
    array, which SQL cannot explode cleanly. Doing that one structural step in
    Python is what makes the staging models straightforward.

        {"2026-08-27": {"1. open": "182.4", ...}, ...}
            ->
        [{"symbol": "IBM", "trade_date": "2026-08-27", "open": "182.4", ...}]

    Args:
        symbol: Ticker the payload was fetched for.
        payload: Full decoded response from fetch().
        series_key: Top-level key holding the series, e.g. "Weekly Time Series".

    Returns:
        Rows sorted by trade_date ascending. Values are all strings.
    """
    extracted_at = datetime.now(timezone.utc).isoformat()
    series = payload[series_key]

    rows = []
    for trade_date, fields in series.items():
        row = {
            "symbol": symbol,
            "trade_date": trade_date,
            "_extracted_at": extracted_at,
            "_source_series": series_key,
        }
        for field_name, value in fields.items():
            row[_safe_column_name(field_name)] = value
        rows.append(row)

    rows.sort(key=lambda r: r["trade_date"])
    return rows


def flatten_overview(symbol: str, payload: dict) -> list[dict]:
    """Turn an OVERVIEW payload into a single row.

    The response is already flat -- every value is a string -- so this only
    sanitises field names and stamps the audit columns. Returns a list so it
    matches the shape load() expects.
    """
    row = {
        "symbol": symbol,
        "_extracted_at": datetime.now(timezone.utc).isoformat(),
        "_source_series": "OVERVIEW",
    }
    for field_name, value in payload.items():
        column = _safe_column_name(field_name)
        # "Symbol" would collide with the "symbol" key we set above; the API
        # value and the ticker we asked for are the same thing.
        if column.lower() == "symbol":
            continue
        row[column] = value
    return [row]


def _client() -> bigquery.Client:
    """Return a cached BigQuery client built from the service-account key."""
    global _bq_client
    if _bq_client is None:
        if not GCP_KEYFILE or not GCP_PROJECT_ID:
            raise RuntimeError(
                "GCP_KEYFILE and GCP_PROJECT_ID must both be set in .env."
            )
        if not Path(GCP_KEYFILE).is_file():
            raise RuntimeError(f"Service-account key not found: {GCP_KEYFILE}")
        _bq_client = bigquery.Client.from_service_account_json(
            GCP_KEYFILE, project=GCP_PROJECT_ID, location=BQ_LOCATION
        )
    return _bq_client


def _schema_for(rows: list[dict]) -> list[bigquery.SchemaField]:
    """Build an all-STRING schema from the union of keys across rows.

    An explicit schema rather than autodetect, because autodetect infers types
    from whatever sample it happens to see and can decide a column is INTEGER
    one day and STRING the next. Pinning everything to STRING makes the raw
    layer stable and hands every typing decision to dbt.
    """
    columns: dict[str, None] = {}
    for row in rows:
        for key in row:
            columns.setdefault(key, None)

    return [
        bigquery.SchemaField(
            name, "TIMESTAMP" if name == "_extracted_at" else "STRING"
        )
        for name in columns
    ]


def load(
    rows: list[dict],
    table_name: str,
    write_disposition: str = "WRITE_APPEND",
) -> int:
    """Load rows into a BigQuery table, creating it if it does not exist.

    Args:
        rows: Records to load. Must be non-empty.
        table_name: Bare table name; the project and raw dataset come from .env.
        write_disposition: WRITE_APPEND keeps every extract for audit and lets
            staging deduplicate. WRITE_TRUNCATE replaces the table instead.

    Returns:
        Number of rows loaded.
    """
    if not rows:
        raise ValueError(f"Refusing to load 0 rows into {table_name}")

    table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=_schema_for(rows),
        write_disposition=write_disposition,
        # Adding a source field later should widen the table, not fail the job.
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ],
    )

    job = _client().load_table_from_json(
        rows, table_id, job_config=job_config, location=BQ_LOCATION
    )
    job.result()  # Blocks, and raises if the load failed.

    log.info("loaded %s rows -> %s", f"{len(rows):,}", table_id)
    return len(rows)


def collect_prices(function: str) -> list[dict]:
    """Fetch and flatten one price series for every ticker.

    A failure on one ticker is logged and skipped rather than aborting the run,
    so a single delisted or rate-limited symbol cannot cost you the other seven.
    """
    series_key, _ = PRICE_SERIES[function]
    rows: list[dict] = []

    for symbol in TICKERS:
        try:
            payload = fetch(symbol, function, expect_key=series_key)
        except (RuntimeError, requests.RequestException) as exc:
            log.warning("skipping %s: %s", symbol, exc)
            continue
        symbol_rows = flatten_prices(symbol, payload, series_key)
        log.info("%-6s %-18s %s rows", symbol, function, len(symbol_rows))
        rows.extend(symbol_rows)

    return rows


def collect_overviews() -> list[dict]:
    """Fetch and flatten OVERVIEW for every ticker."""
    rows: list[dict] = []

    for symbol in TICKERS:
        try:
            payload = fetch(symbol, "OVERVIEW", expect_key="Name")
        except (RuntimeError, requests.RequestException) as exc:
            log.warning("skipping %s: %s", symbol, exc)
            continue
        rows.extend(flatten_overview(symbol, payload))
        log.info("%-6s %-18s ok", symbol, "OVERVIEW")

    return rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load Alpha Vantage data into the BigQuery raw dataset."
    )
    parser.add_argument(
        "--daily", action="store_true", help="last 100 trading sessions"
    )
    parser.add_argument(
        "--weekly", action="store_true", help="full weekly history"
    )
    parser.add_argument(
        "--overview", action="store_true", help="company profile per ticker"
    )
    parser.add_argument(
        "--all", action="store_true", help="all three (24 API calls)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="fetch and flatten but do not write to BigQuery",
    )
    args = parser.parse_args()

    if args.all:
        args.daily = args.weekly = args.overview = True
    if not (args.daily or args.weekly or args.overview):
        parser.error("pick at least one of --daily, --weekly, --overview, --all")

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s"
    )

    jobs: list[tuple[str, list[dict]]] = []
    if args.daily:
        jobs.append((PRICE_SERIES["TIME_SERIES_DAILY"][1],
                     collect_prices("TIME_SERIES_DAILY")))
    if args.weekly:
        jobs.append((PRICE_SERIES["TIME_SERIES_WEEKLY"][1],
                     collect_prices("TIME_SERIES_WEEKLY")))
    if args.overview:
        jobs.append((OVERVIEW_TABLE, collect_overviews()))

    empty = [table for table, rows in jobs if not rows]
    if empty:
        log.error("no rows collected for: %s", ", ".join(empty))
        return 1

    for table_name, rows in jobs:
        if args.dry_run:
            log.info("[dry-run] would load %s rows -> %s", len(rows), table_name)
            log.info("[dry-run] first row: %s", rows[0])
        else:
            load(rows, table_name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
