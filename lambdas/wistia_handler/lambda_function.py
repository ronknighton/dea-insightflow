"""
InsightFlow - Wistia scheduled handler.

Triggered by EventBridge Scheduler on a recurring cadence (see
infrastructure/wistia.yaml). Per Section 8 of the InsightFlow Solution
Design doc, for each of the two tracked media IDs:

  1. Pull the current engagement snapshot (stats/medias/{id}/engagement.json)
     - this is always an all-time aggregate, so it's simply re-fetched and
     overwritten each run; there's nothing to paginate or watermark here.
  2. Pull new visitor-level events (stats/events.json) since the last run,
     using SSM-stored watermarks per media_id.

AUTH CORRECTION FROM THE DESIGN DOC: Section 4 described this as "token-
based Basic Auth." Checking Wistia's current API docs directly
(docs.wistia.com/docs/making-api-requests) shows Bearer token in the
Authorization header is the officially supported method ("The supported
way to access the API is via Bearer Token"); HTTP Basic (username "api",
token as password) is only listed as an alternative. This Lambda uses
Bearer, and the design doc should be corrected to match.

INCREMENTAL WATERMARKING, WHY CLIENT-SIDE: Wistia's list endpoints support
page/per_page and sort_by/sort_direction, but no documented "since
timestamp" query filter. So incremental pull here means: request events
sorted newest-first, page through, and stop as soon as a page's oldest
record is at or before the stored watermark - not a single filtered
request. The new watermark is the newest received_at seen this run.

ASSUMPTION FLAGGED: the exact events.json response shape and whether
media_id is a valid filter query param were not confirmed against a live
account at the time this was written (Wistia's public docs describe the
concept - "Events List" - without a fully specified parameter reference in
what was accessible while building this). _fetch_events_page() isolates
this so it can be corrected without touching the incremental-stop logic
or anything downstream.

Environment variables:
  BUCKET_NAME       - InsightFlow data bucket
  WISTIA_TOKEN_ARN  - Secrets Manager ARN of the Wistia API token
  MEDIA_IDS         - comma-separated list of media IDs to track (default: the two given in the brief)
"""

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
ssm_client = boto3.client("ssm")

BUCKET_NAME = os.environ.get("BUCKET_NAME")
WISTIA_TOKEN_ARN = os.environ.get("WISTIA_TOKEN_ARN")
MEDIA_IDS = [m.strip() for m in os.environ.get("MEDIA_IDS", "8hunphufxp,9k4tbcdfg0").split(",") if m.strip()]
WISTIA_API_BASE = "https://api.wistia.com/v1"
PER_PAGE = 100
MAX_PAGES_PER_RUN = 10  # safety ceiling - avoids a runaway pull if the watermark is ever wrong/missing

_cached_token = None


def _get_token() -> str:
    global _cached_token
    if _cached_token is not None:
        return _cached_token
    try:
        response = secrets_client.get_secret_value(SecretId=WISTIA_TOKEN_ARN)
    except ClientError:
        logger.exception("Failed to retrieve Wistia API token from Secrets Manager")
        raise
    _cached_token = response["SecretString"]
    return _cached_token


def _api_get(path: str, params: dict = None) -> dict:
    url = f"{WISTIA_API_BASE}/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {_get_token()}"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _watermark_param_name(media_id: str) -> str:
    return f"/{os.environ.get('PROJECT_NAME', 'insightflow')}/wistia/watermark/{media_id}"


def _get_watermark(media_id: str) -> str:
    """Returns the ISO timestamp of the newest event already ingested for
    this media, or None if this media has never been pulled before."""
    try:
        response = ssm_client.get_parameter(Name=_watermark_param_name(media_id))
        return response["Parameter"]["Value"]
    except ssm_client.exceptions.ParameterNotFound:
        return None


def _set_watermark(media_id: str, timestamp: str) -> None:
    ssm_client.put_parameter(
        Name=_watermark_param_name(media_id),
        Value=timestamp,
        Type="String",
        Overwrite=True,
    )


def _fetch_engagement_snapshot(media_id: str) -> dict:
    return _api_get(f"stats/medias/{media_id}/engagement.json")


def _fetch_events_page(media_id: str, page: int) -> list:
    """ASSUMPTION FLAGGED - see module docstring. media_id as a filter
    param and the exact response shape (bare list vs. wrapped) are the
    parts most likely to need correcting against a live account."""
    result = _api_get(
        "stats/events.json",
        params={
            "media_id": media_id,
            "page": page,
            "per_page": PER_PAGE,
            "sort_by": "received_at",
            "sort_direction": 0,  # descending - newest first, required for the watermark-stop logic below
        },
    )
    return result if isinstance(result, list) else result.get("events", [])


def _fetch_new_events(media_id: str) -> list:
    """Paginates newest-first, stopping at the stored watermark (or after
    MAX_PAGES_PER_RUN as a safety ceiling). Returns only events newer than
    the watermark."""
    watermark = _get_watermark(media_id)
    new_events = []
    newest_seen = watermark

    for page in range(1, MAX_PAGES_PER_RUN + 1):
        events = _fetch_events_page(media_id, page)
        if not events:
            break

        if page == 1 and events:
            newest_seen = events[0].get("received_at", newest_seen)

        reached_watermark = False
        for event in events:
            received_at = event.get("received_at")
            if watermark is not None and received_at is not None and received_at <= watermark:
                reached_watermark = True
                break
            new_events.append(event)

        if reached_watermark or len(events) < PER_PAGE:
            break

    return new_events, newest_seen


def _write_raw(media_id: str, kind: str, data) -> str:
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/wistia_stats/media_id={media_id}/dt={ingestion_date}/{kind}.json"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def _process_one_media(media_id: str) -> dict:
    engagement = _fetch_engagement_snapshot(media_id)
    _write_raw(media_id, "engagement", engagement)

    new_events, newest_seen = _fetch_new_events(media_id)
    if new_events:
        _write_raw(media_id, f"events_{datetime.now(timezone.utc).strftime('%H%M%S')}", new_events)
        if newest_seen:
            _set_watermark(media_id, newest_seen)

    return {"media_id": media_id, "new_event_count": len(new_events)}


def lambda_handler(event, context):
    results = []
    for media_id in MEDIA_IDS:
        try:
            results.append(_process_one_media(media_id))
        except Exception:
            logger.exception("Failed to process media_id=%s", media_id)
            results.append({"media_id": media_id, "error": True})

    logger.info("Wistia pull complete: %s", results)
    return {"results": results}
