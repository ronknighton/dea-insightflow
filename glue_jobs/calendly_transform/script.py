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

# PRIMARY channel-matching mechanism (Aug 18, 2026 rebuild) - matches on
# scheduled_event.event_type, per the requirements doc's own stated
# intent ("event_type: This will help you identify the marketing
# campaigns events"), NOT tracking.utm_campaign. utm_campaign was the
# original approach this project built first; it turned out to be broken
# in practice (391/407 real bookings null, the remaining 16 contain IP
# addresses, not channel names - see Section 18, Item 8 of the design
# doc) AND was never actually the mechanism the brief specified.
#
# The three event_type IDs given in the requirements doc were checked
# directly against real, current booking data and found to be STALE for
# two of three channels - Calendly assigns a new UUID whenever an event
# type is deleted and recreated, and nothing in the webhook payload
# indicates that drift happened:
#   - youtube_paid_ads: doc's reference ID CONFIRMED correct against real
#     data (6 real bookings, "Info Session (YT)").
#   - facebook_paid_ads: doc's reference ID matches only 1 historical
#     booking (almost certainly the doc's own original sample event,
#     still present in the data) - NOT what real, current Facebook
#     bookings actually use. Three different, currently-active event
#     type IDs were found in real data with "FB" in the meeting name.
#   - tiktok_paid_ads: doc's reference ID matches ZERO real bookings.
#     Two different, currently-active event type IDs were found with
#     "TC" in the meeting name instead.
#
# This map is therefore built from real, current, verified data - not
# solely the (partially stale) requirements doc. The doc's original
# three reference IDs are kept in the map too (harmless if never used
# again; correct if they ever are).
EVENT_TYPE_CHANNEL_MAP = {
    # Facebook - real, currently-active event types (verified Aug 18, 2026)
    "13b9e08f-19d6-4632-99c5-4b213dbc335f": "facebook_paid_ads",  # "Breakthrough Session FB D2C"
    "91e2e844-449d-41a5-b54a-1446d91abdcc": "facebook_paid_ads",  # "Info Session (FB FT/V)"
    "cbb0d033-c0e9-4cc1-998c-87b224561a33": "facebook_paid_ads",  # "Info Session FB Multi Opt FT/V"
    "d639ecd3-8718-4068-955a-436b10d72c78": "facebook_paid_ads",  # requirements doc's original reference - stale but kept
    # YouTube - doc's reference confirmed correct against real data
    "dbb4ec50-38cd-4bcd-bbff-efb7b5a6f098": "youtube_paid_ads",  # "Info Session (YT)"
    # TikTok - real, currently-active event types (verified Aug 18, 2026)
    "789dcd61-4362-4ecf-a99a-553853075620": "tiktok_paid_ads",  # "Breakthrough Session TC AN"
    "79a72e89-978b-493c-84ba-9c0db9fd8435": "tiktok_paid_ads",  # "Breakthrough Session (TC)"
    "bb339e98-7a67-4af2-b584-8dbf95564312": "tiktok_paid_ads",  # requirements doc's original reference - unused in real data so far, kept
}


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


def _extract_channel(payload: dict, event_type_id: str = None) -> str:
    """PRIMARY: match event_type_id against EVENT_TYPE_CHANNEL_MAP - see
    the map's own comment for why this replaced tracking.utm_campaign as
    the authoritative mechanism. FALLBACK: the original utm_campaign/
    Q&A-based checks are kept for defense-in-depth even though real data
    has shown utm_campaign is populated with IP addresses, not channel
    names, on every real booking checked so far - costs nothing to leave
    in case that ever changes upstream."""
    if event_type_id and event_type_id in EVENT_TYPE_CHANNEL_MAP:
        return EVENT_TYPE_CHANNEL_MAP[event_type_id]

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
    event_type_id = _event_type_id_from_url(event_type_url)

    return {
        "booking_id": _booking_id_from_uri(payload.get("uri")),
        "invitee_name": payload.get("name"),
        "invitee_email": payload.get("email"),
        "channel": _extract_channel(payload, event_type_id),
        "campaign_raw": (payload.get("tracking", {}) or {}).get("utm_campaign"),
        "event_type_url": event_type_url,
        "event_type_id": event_type_id,
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

        # CONFIRMED PRODUCTION BUG (Aug 18, 2026): each daily spend file
        # covers a rolling ~30-day window (per the requirements doc:
        # "Data in the file ranges for a time period of 30 days at
        # maximum"), and this job reads every raw file on every
        # full-rebuild run. A single (channel, date) fact therefore
        # appears in every overlapping file that ever included it -
        # confirmed directly against real data: up to 8 duplicate rows
        # for the same channel+date, inflating every marts-layer join
        # against calendly_bookings by the same multiplier (bookings and
        # spend both fan out once per duplicate spend row).
        before = len(df)
        # Sanity check BEFORE dropping: if the "duplicate" rows actually
        # disagree on spend for the same (channel, date), that's a
        # different, more concerning problem (a real revision to
        # historical spend) that silently keeping "first" would hide.
        conflicting = df.groupby(["channel", "date"])["spend"].nunique()
        conflicting_keys = conflicting[conflicting > 1]
        if not conflicting_keys.empty:
            logger.warning(
                "%d (channel, date) pairs have DIFFERING spend values across duplicate "
                "files, not just repeated identical ones - dropping to 'first' may be "
                "hiding a real data revision, not just redundant overlap: %s",
                len(conflicting_keys), conflicting_keys.index.tolist()[:10],
            )

        df = df.drop_duplicates(subset=["channel", "date"], keep="first")
        removed = before - len(df)
        if removed:
            logger.info(
                "Removed %d duplicate (channel, date) rows from overlapping spend "
                "files (%d -> %d rows)", removed, before, len(df),
            )
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