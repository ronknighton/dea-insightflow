"""
InsightFlow - Calendly Glue Python Shell transform job.

Per Section 7 of the InsightFlow Solution Design doc: reads all raw Calendly
webhook events and all raw spend files, flattens/standardizes each into its
own clean Parquet table under processed/. Does NOT join bookings to spend -
that join, plus the metric computation in Section 9, happens later via
Athena CTAS at the gold (marts/) layer, not here.

FULL-REBUILD PATTERN, not incremental: every run reads everything currently
under raw/calendly_webhook_events/ and raw/calendly_spend/ and completely
overwrites processed/calendly_bookings/ and processed/calendly_spend/.
Mirrors the Healthcare Metrics precedent documented in Section 6 of the
design doc - simpler than partition-level incremental merge, and cheap
enough at this data volume. Revisit if volume grows materially.

Runs as an AWS Glue Python Shell job (PythonVersion 3.9), which comes with
pandas and awswrangler pre-installed - no custom layer/packaging needed,
unlike the Lambda functions in this project. Triggered by EventBridge
Scheduler (infrastructure/calendly-transform-job.yaml).

Job parameters (passed as --KEY value by the Glue job definition, not
environment variables - see _get_job_param()):
  --BUCKET_NAME  InsightFlow data bucket

ASSUMPTION FLAGGED: "campaign" is called out as a distinct dimension from
"channel" in the requirements doc's leaderboard deliverable (source,
campaign, booking_id, spend), but the sample webhook payload only exposes
tracking.utm_campaign, which in this dataset holds the channel value itself
(e.g. "facebook_paid_ads") - there's no separate campaign-name field
visible in the sample data. campaign_raw below is deliberately named to
signal it may not be a distinct dimension from channel; revisit if a
richer sample payload surfaces a real campaign identifier.
"""

import json
import logging
import sys
from urllib.parse import urlparse

import awswrangler as wr
import boto3
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")

TRACKED_EVENT_TYPES = {"facebook_paid_ads", "youtube_paid_ads", "tiktok_paid_ads"}


def _get_job_param(name: str, default: str = None) -> str:
    """Glue Python Shell jobs receive parameters as --KEY value command-line
    args, not environment variables. Deliberately not using
    awsglue.utils.getResolvedOptions here so this script has zero
    Glue-runtime-only imports and can be unit tested with plain Python."""
    flag = f"--{name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return default


def _list_raw_keys(bucket: str, prefix: str) -> list:
    """Paginated listing - a multi-day raw/ prefix can exceed the 1000-key
    single-page limit."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys


def _read_json(bucket: str, key: str):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def _extract_channel(payload: dict) -> str:
    """Mirrors calendly_webhook_handler's _extract_channel - re-derived
    here rather than trusted from upstream, since raw/ is the actual
    source of truth and transform logic shouldn't assume the ingestion
    Lambda's filtering never changes."""
    tracking = payload.get("tracking", {}) or {}
    for field in ("utm_campaign", "utm_source"):
        value = tracking.get(field)
        if value in TRACKED_EVENT_TYPES:
            return value
    for qa in payload.get("questions_and_answers", []) or []:
        question = (qa.get("question") or "").lower()
        answer = (qa.get("answer") or "").strip()
        if ("channel" in question or "source" in question) and answer in TRACKED_EVENT_TYPES:
            return answer
    return None


def _booking_id_from_uri(uri: str) -> str:
    """The invitee UUID is the final path segment of payload.uri - same
    extraction the webhook Lambda uses for the S3 filename."""
    if not uri:
        return None
    return urlparse(uri).path.rstrip("/").split("/")[-1]


def _event_type_id_from_url(url: str) -> str:
    """event_type is Calendly's real, deterministic identifier for which
    booking page/campaign this event belongs to - per the requirements
    doc, this is the intended channel-matching mechanism, NOT
    tracking.utm_campaign (which this project spent a long time chasing
    before finding this). Same trailing-path-segment extraction as
    _booking_id_from_uri."""
    if not url:
        return None
    return urlparse(url).path.rstrip("/").split("/")[-1]


def _flatten_booking_event(event_body: dict, source_key: str) -> dict:
    payload = event_body.get("payload", {}) or {}
    scheduled_event = payload.get("scheduled_event", {}) or {}
    memberships = scheduled_event.get("event_memberships", []) or []
    first_host = memberships[0] if memberships else {}
    event_type_url = scheduled_event.get("event_type")

    return {
        "booking_id": _booking_id_from_uri(payload.get("uri")),
        "invitee_name": payload.get("name"),
        "invitee_email": payload.get("email"),
        "channel": _extract_channel(payload),
        "campaign_raw": (payload.get("tracking", {}) or {}).get("utm_campaign"),
        "event_type_url": event_type_url,
        "event_type_id": _event_type_id_from_url(event_type_url),
        "booked_at": payload.get("created_at"),
        "meeting_name": scheduled_event.get("name"),
        "meeting_start_time": scheduled_event.get("start_time"),
        "meeting_end_time": scheduled_event.get("end_time"),
        "employee_email": first_host.get("user_email"),
        "employee_name": first_host.get("user_name"),
        "timezone": payload.get("timezone"),
        "status": payload.get("status"),
        "rescheduled": payload.get("rescheduled"),
        "source_file": source_key,
    }


def transform_bookings(bucket: str) -> pd.DataFrame:
    keys = _list_raw_keys(bucket, "raw/calendly_webhook_events/")
    logger.info("Found %d raw booking event files", len(keys))

    rows = []
    for key in keys:
        try:
            event_body = _read_json(bucket, key)
            rows.append(_flatten_booking_event(event_body, key))
        except Exception:
            logger.exception("Skipping unreadable booking event: %s", key)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["booked_at"] = pd.to_datetime(df["booked_at"], errors="coerce", utc=True)
        df["meeting_start_time"] = pd.to_datetime(df["meeting_start_time"], errors="coerce", utc=True)
        df["meeting_end_time"] = pd.to_datetime(df["meeting_end_time"], errors="coerce", utc=True)
        # Explicit string dtype, not left to pyarrow's inference. Root
        # cause of a real production failure (Aug 15, 2026): with every
        # real booking captured so far having channel=None, a column
        # that's 100% null gives pyarrow no actual string data to infer a
        # type from - it defaulted to something else entirely (cataloged
        # as INTEGER), breaking every marts-layer query that compared or
        # coalesced this column against a real string value. pandas'
        # nullable "string" dtype has a well-defined pyarrow mapping
        # regardless of null content, unlike generic "object" dtype.
        # campaign_raw mirrors utm_campaign and is equally all-null right
        # now, so it gets the same treatment defensively.
        df["channel"] = df["channel"].astype("string")
        df["campaign_raw"] = df["campaign_raw"].astype("string")
        # event_type_url/id are populated on nearly every real booking (not
        # a rare, mostly-null field like channel/campaign_raw), so the
        # empty-column type-inference bug is less likely here - but the
        # fix costs nothing to apply defensively regardless.
        df["event_type_url"] = df["event_type_url"].astype("string")
        df["event_type_id"] = df["event_type_id"].astype("string")
    return df


def _flatten_spend_records(spend_file: list, source_key: str) -> list:
    rows = []
    for record in spend_file:
        rows.append({
            "date": record.get("date"),
            "channel": record.get("channel"),
            "spend": record.get("spend"),
            "source_file": source_key,
        })
    return rows


def transform_spend(bucket: str) -> pd.DataFrame:
    keys = _list_raw_keys(bucket, "raw/calendly_spend/")
    logger.info("Found %d raw spend files", len(keys))

    rows = []
    for key in keys:
        try:
            spend_file = _read_json(bucket, key)
            rows.extend(_flatten_spend_records(spend_file, key))
        except Exception:
            logger.exception("Skipping unreadable spend file: %s", key)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df["spend"] = pd.to_numeric(df["spend"], errors="coerce")
    return df


def main():
    bucket = _get_job_param("BUCKET_NAME")
    if not bucket:
        raise ValueError("--BUCKET_NAME job parameter is required")

    bookings_df = transform_bookings(bucket)
    if not bookings_df.empty:
        wr.s3.to_parquet(
            df=bookings_df,
            path=f"s3://{bucket}/processed/calendly_bookings/",
            dataset=True,
            mode="overwrite",
        )
        logger.info("Wrote %d booking rows to processed/calendly_bookings/", len(bookings_df))
    else:
        logger.info("No booking events found - skipping write (leaving prior processed/ output untouched)")

    spend_df = transform_spend(bucket)
    if not spend_df.empty:
        wr.s3.to_parquet(
            df=spend_df,
            path=f"s3://{bucket}/processed/calendly_spend/",
            dataset=True,
            mode="overwrite",
        )
        logger.info("Wrote %d spend rows to processed/calendly_spend/", len(spend_df))
    else:
        logger.info("No spend records found - skipping write (leaving prior processed/ output untouched)")


if __name__ == "__main__":
    main()