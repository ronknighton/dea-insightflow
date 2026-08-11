"""
InsightFlow - Calendly ad-spend scheduled handler.

Triggered by EventBridge Scheduler on a recurring cadence (see
infrastructure/calendly-spend.yaml) during the window after the spend
file's expected ~06:00 EST publish time. Per Section 7 of the InsightFlow
Solution Design doc:

  1. Check file_index.json to confirm today's Day-1 spend file is actually
     published before attempting to pull it - the file does not appear
     instantly at a fixed time, so this Lambda is scheduled to run several
     times across a window and no-ops gracefully if the file isn't there
     yet (the "short backoff" in the design doc is the EventBridge
     schedule's recurrence, not an in-Lambda sleep loop - avoids paying
     for idle Lambda billed duration while waiting).
  2. Idempotency: if today's Day-1 file has already been pulled into raw/
     (checked by S3 key existence, same ListBucket + GetObject pattern
     used in the CRM consumer's idempotency check - see that module's
     docstring for why ListBucket is required to get a real 404 instead of
     a masked 403), this run is a no-op.
  3. Pull the file and write it unmodified to
     raw/calendly_spend/dt=YYYY-MM-DD/spend_data_{Day-1}.json - raw stays
     an untouched mirror of the source, consistent with every other
     ingestion Lambda in this pipeline.

ASSUMPTION FLAGGED: the exact base URL and file_index.json structure were
not confirmed against the requirements doc's literal example at the time
this was written (same category of gap as CRM consumer's
LEAD_OWNER_BASE_URL). CALENDLY_SPEND_BASE_URL and the index-parsing logic
in _is_file_available() are environment-variable-driven and isolated in
one function specifically so this can be corrected without touching
anything else, once verified against a real sample.

Environment variables:
  BUCKET_NAME               - InsightFlow data bucket
  CALENDLY_SPEND_BASE_URL   - base URL hosting the daily spend files + file_index.json
"""

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

BUCKET_NAME = os.environ.get("BUCKET_NAME")
CALENDLY_SPEND_BASE_URL = os.environ.get(
    "CALENDLY_SPEND_BASE_URL", "https://dea-calendly-spend.s3.amazonaws.com"
)


def _day_minus_1() -> str:
    """
    Yesterday's date, UTC-based. This function is scheduled to run well
    after EST/EDT midnight (see the EventBridge schedule window in
    calendly-spend.yaml), so a UTC-date computation is safe and avoids
    pulling in a timezone database dependency for a distinction that
    doesn't actually matter at this run time.
    """
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def _is_file_available(day: str) -> bool:
    """Checks file_index.json for the expected filename rather than
    guessing the file exists and handling a 404 - the design doc commits
    to this pattern specifically because a missing file here is routine
    (publish timing varies), not exceptional."""
    index_url = f"{CALENDLY_SPEND_BASE_URL}/file_index.json"
    expected_filename = f"spend_data_{day}.json"

    try:
        with urllib.request.urlopen(index_url, timeout=5) as response:
            index = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError):
        logger.warning("Could not read file_index.json - treating as not yet available", exc_info=True)
        return False

    # ASSUMPTION FLAGGED: assumes index is a JSON list of filename strings.
    # If the real structure differs (e.g. list of objects with a "name"
    # field, or a dict keyed by date), this check needs updating.
    available_files = index if isinstance(index, list) else index.get("files", [])
    return expected_filename in available_files


def _already_ingested(day: str) -> bool:
    key = f"raw/calendly_spend/dt={day}/spend_data_{day}.json"
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def _pull_and_write(day: str) -> None:
    file_url = f"{CALENDLY_SPEND_BASE_URL}/spend_data_{day}.json"
    with urllib.request.urlopen(file_url, timeout=10) as response:
        raw_body = response.read()

    key = f"raw/calendly_spend/dt={day}/spend_data_{day}.json"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=raw_body,
        ContentType="application/json",
    )
    logger.info("Wrote %s (%d bytes)", key, len(raw_body))


def lambda_handler(event, context):
    day = _day_minus_1()

    if _already_ingested(day):
        logger.info("Spend file for %s already ingested - no-op", day)
        return {"status": "already_ingested", "day": day}

    if not _is_file_available(day):
        logger.info("Spend file for %s not yet published - will retry on next scheduled run", day)
        return {"status": "not_yet_available", "day": day}

    _pull_and_write(day)
    return {"status": "ingested", "day": day}
