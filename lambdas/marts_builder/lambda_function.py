"""
InsightFlow - Athena marts builder.

Runs CTAS (CREATE TABLE AS SELECT) queries against Athena, computing the
Calendly business metrics (Section 9) and the cross-source
channel_performance_summary (Section 10) directly from the already-
cataloged processed/ tables.

FULL-REBUILD PATTERN, matching every other component in this project -
but CTAS has a real constraint the others don't: Athena's CTAS FAILS
OUTRIGHT if its target S3 location already has any data in it (confirmed
against AWS's own docs - unlike awswrangler's mode="overwrite" used
elsewhere in this project, native CTAS has no overwrite option). So each
mart's rebuild is a three-step sequence, not just re-running the CREATE:
  1. DROP TABLE IF EXISTS <mart> (removes the Glue Catalog entry only -
     does NOT delete the underlying S3 data)
  2. Delete every object under marts/<mart>/ directly via S3
  3. Run the CTAS to recreate the table fresh

NULL CHANNEL HANDLING: every real Calendly booking observed as of this
writing has channel=None (see Section 14 open item - no positive example
of a paid-ad-attributed booking has been seen yet, only organic/referred
ones). Metrics that group by channel use COALESCE(channel,
'organic_unknown') so these bookings form a visible, countable bucket
instead of silently vanishing from GROUP BY results (which is what would
happen if NULL rows were naively excluded) - genuinely useful signal, not
just a null-handling nicety, given what real traffic looks like so far.

ASSUMPTION FLAGGED - date typing across sources is NOT uniform, and this
matters for the SQL below:
  - calendly_bookings.booked_at / meeting_start_time / meeting_end_time
    ARE real Parquet TIMESTAMP columns (converted via pandas.to_datetime
    in the Glue transform script) - safe to use directly with
    date_trunc(), hour(), day_of_week(), etc.
  - calendly_spend.date is a Python date object column (pandas .dt.date)
    - LIKELY typed as DATE in the catalog, but less certain than a native
    datetime64 column; not independently verified against a live Athena
    query at the time this was written.
  - crm_leads_enriched.date_created / processed_at were NEVER converted
    via pandas.to_datetime (confirmed by reading crm_consumer_handler's
    source directly) - these are plain ISO-8601 STRINGS in the catalog,
    not timestamps. SUBSTR(date_created, 1, 10) is used below to extract
    just the YYYY-MM-DD date portion for grouping, deliberately avoiding
    a fragile date_parse() format-string guess - safe for grouping, not
    suitable for anything needing hour/minute precision.
Every mart below should be spot-checked against its first real run before
being trusted for reporting - this is genuinely new, unverified SQL.

CONFIRMED AGAINST LIVE DATA (Aug 15, 2026): calendly_bookings.channel got
cataloged as INTEGER, not VARCHAR - a direct consequence of every real
Calendly booking so far having channel=None. With 100% null values,
pyarrow has no actual string data to infer a type from and can default to
something else entirely (a well-documented Parquet/pyarrow quirk, not a
bug in this project's code specifically). Every query below that
compares or coalesces calendly_bookings.channel wraps it in
CAST(channel AS VARCHAR) defensively - this protects against the type
mismatch regardless of what the catalog says, and costs nothing once
real string channel values eventually appear. The actual root cause is
also fixed at the source in glue_jobs/calendly_transform/script.py
(explicit dtype forcing), but the defensive CAST here stays either way -
belt and suspenders, given how this exact quirk could recur for any
other currently-all-null column (e.g. campaign_raw).

Job parameters come from environment variables (standard Lambda config,
not Glue-style --KEY value args):
  GLUE_DATABASE           - Glue Catalog database name (default: insightflow)
  BUCKET_NAME              - InsightFlow data bucket
  ATHENA_WORKGROUP         - Athena workgroup name
  ATHENA_RESULTS_LOCATION  - s3://.../athena-results/ (query result staging, separate from mart output)
"""

import logging
import os
import time

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena_client = boto3.client("athena")
s3_client = boto3.client("s3")

GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "insightflow")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP")
ATHENA_RESULTS_LOCATION = os.environ.get("ATHENA_RESULTS_LOCATION")

POLL_INTERVAL_SECONDS = 2
MAX_WAIT_SECONDS = 120

MART_QUERIES = {
    "daily_calls_booked_by_source": """
        SELECT
            CAST(booked_at AS DATE) AS booking_date,
            COALESCE(CAST(channel AS VARCHAR), 'organic_unknown') AS source,
            COUNT(booking_id) AS bookings
        FROM calendly_bookings
        GROUP BY CAST(booked_at AS DATE), COALESCE(CAST(channel AS VARCHAR), 'organic_unknown')
    """,
    "cost_per_booking_by_channel": """
        SELECT
            s.channel,
            SUM(s.spend) AS total_spend,
            COUNT(DISTINCT b.booking_id) AS bookings,
            SUM(s.spend) / NULLIF(COUNT(DISTINCT b.booking_id), 0) AS cost_per_booking
        FROM calendly_spend s
        LEFT JOIN calendly_bookings b
            ON CAST(b.channel AS VARCHAR) = s.channel
           AND CAST(b.booked_at AS DATE) = s.date
        GROUP BY s.channel
    """,
    "bookings_trend_over_time": """
        SELECT
            CAST(booked_at AS DATE) AS booking_date,
            COALESCE(CAST(channel AS VARCHAR), 'organic_unknown') AS source,
            COUNT(booking_id) AS bookings,
            SUM(COUNT(booking_id)) OVER (
                PARTITION BY COALESCE(CAST(channel AS VARCHAR), 'organic_unknown')
                ORDER BY CAST(booked_at AS DATE)
            ) AS cumulative_bookings
        FROM calendly_bookings
        GROUP BY CAST(booked_at AS DATE), COALESCE(CAST(channel AS VARCHAR), 'organic_unknown')
    """,
    "channel_attribution": """
        SELECT
            COALESCE(CAST(b.channel AS VARCHAR), 'organic_unknown') AS source,
            b.campaign_raw AS campaign,
            COUNT(b.booking_id) AS bookings,
            COALESCE(SUM(s.spend), 0) AS total_spend,
            COALESCE(SUM(s.spend), 0) / NULLIF(COUNT(b.booking_id), 0) AS cost_per_booking
        FROM calendly_bookings b
        LEFT JOIN calendly_spend s
            ON CAST(b.channel AS VARCHAR) = s.channel
           AND CAST(b.booked_at AS DATE) = s.date
        GROUP BY COALESCE(CAST(b.channel AS VARCHAR), 'organic_unknown'), b.campaign_raw
    """,
    "booking_volume_by_timeslot": """
        SELECT
            HOUR(booked_at) AS hour_of_day,
            DAY_OF_WEEK(booked_at) AS day_of_week,
            COUNT(booking_id) AS bookings
        FROM calendly_bookings
        GROUP BY HOUR(booked_at), DAY_OF_WEEK(booked_at)
    """,
    "meeting_load_per_employee": """
        SELECT
            employee_email,
            employee_name,
            COUNT(booking_id) AS total_meetings,
            COUNT(DISTINCT WEEK(booked_at)) AS distinct_weeks,
            CAST(COUNT(booking_id) AS DOUBLE) / NULLIF(COUNT(DISTINCT WEEK(booked_at)), 0) AS meetings_per_week
        FROM calendly_bookings
        WHERE employee_email IS NOT NULL
        GROUP BY employee_email, employee_name
    """,
    "channel_performance_summary": """
        SELECT
            b.channel,
            b.booking_date,
            b.bookings,
            COALESCE(l.leads_created, 0) AS leads_created
        FROM (
            SELECT
                COALESCE(CAST(channel AS VARCHAR), 'organic_unknown') AS channel,
                CAST(booked_at AS DATE) AS booking_date,
                COUNT(booking_id) AS bookings
            FROM calendly_bookings
            GROUP BY COALESCE(CAST(channel AS VARCHAR), 'organic_unknown'), CAST(booked_at AS DATE)
        ) b
        LEFT JOIN (
            SELECT
                DATE(SUBSTR(date_created, 1, 10)) AS lead_date,
                COUNT(lead_id) AS leads_created
            FROM crm_leads_enriched
            WHERE date_created IS NOT NULL
            GROUP BY SUBSTR(date_created, 1, 10)
        ) l
            ON b.booking_date = l.lead_date
    """,
}


def _wait_for_query(query_execution_id: str) -> None:
    elapsed = 0
    while elapsed < MAX_WAIT_SECONDS:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELLED"):
            reason = response["QueryExecution"]["Status"].get("StateChangeReason", "no reason given")
            raise RuntimeError(f"Athena query {query_execution_id} {state}: {reason}")

        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    raise TimeoutError(f"Athena query {query_execution_id} did not complete within {MAX_WAIT_SECONDS}s")


def _run_query(sql: str) -> str:
    response = athena_client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": GLUE_DATABASE},
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={"OutputLocation": ATHENA_RESULTS_LOCATION},
    )
    query_execution_id = response["QueryExecutionId"]
    _wait_for_query(query_execution_id)
    return query_execution_id


def _delete_all_objects(prefix: str) -> int:
    deleted = 0
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue
        keys = [{"Key": obj["Key"]} for obj in objects]
        s3_client.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": keys})
        deleted += len(keys)
    return deleted


def _rebuild_mart(mart_name: str, select_sql: str) -> dict:
    logger.info("Rebuilding mart: %s", mart_name)

    try:
        _run_query(f"DROP TABLE IF EXISTS {mart_name}")
    except RuntimeError:
        logger.exception("DROP TABLE failed for %s - continuing, CTAS will surface the real problem if one exists", mart_name)

    prefix = f"marts/{mart_name}/"
    deleted_count = _delete_all_objects(prefix)
    logger.info("Deleted %d prior objects under marts/%s/", deleted_count, mart_name)

    location = f"s3://{BUCKET_NAME}/marts/{mart_name}/"
    ctas_sql = f"""
        CREATE TABLE {mart_name}
        WITH (format = 'PARQUET', external_location = '{location}')
        AS {select_sql}
    """
    _run_query(ctas_sql)
    logger.info("Successfully rebuilt mart: %s", mart_name)
    return {"mart": mart_name, "status": "success"}


def lambda_handler(event, context):
    results = []
    for mart_name, select_sql in MART_QUERIES.items():
        try:
            results.append(_rebuild_mart(mart_name, select_sql))
        except Exception as e:
            logger.exception("Failed to rebuild mart: %s", mart_name)
            results.append({"mart": mart_name, "status": "error", "error": str(e)})

    logger.info("Marts build complete: %s", results)
    return {"results": results}