"""
InsightFlow - Calendly webhook handler.

Triggered by API Gateway on every Calendly webhook POST (invitee.created).

Responsibilities, per Section 7 of the InsightFlow Solution Design doc:
  1. Validate the Calendly-Webhook-Signature header (Calendly's documented
     HMAC-SHA256 scheme: t=<timestamp>,v1=<hex signature>, signed over
     "{timestamp}.{raw_body}" - see
     https://developer.calendly.com/api-docs/webhook-signatures).
  2. Write the raw, unmodified event to S3
     raw/calendly_webhook_events/dt=YYYY-MM-DD/invitee_{invitee_id}.json.
     Unlike CRM, there is no delay/lookup step downstream - the join to
     spend data happens later in Glue/Athena, not per-event here.

REVISED (Aug 14, 2026) - channel filtering moved OUT of this Lambda: an
earlier version rejected/discarded events whose channel couldn't be
matched to the three tracked paid-ad types before ever writing to raw/.
That silently lost every real Calendly booking once real traffic started
arriving - the channel-matching guess (see ASSUMPTION FLAGGED below) was
wrong for real payloads, and because the reject happened before the S3
write, there was no artifact left to even diagnose what the real payload
looked like. This violates the same "raw stays an untouched, complete
mirror" principle every other source in this project follows (CRM,
Wistia). Channel is now purely informational at this layer - logged, and
included (possibly as null) in the response - never a reason to skip the
write. Classification belongs in the Glue transform job
(glue_jobs/calendly_transform/script.py), which already re-derives it
independently and can be corrected/backfilled against already-landed raw
data, unlike a one-shot ingestion-time decision.

ASSUMPTION FLAGGED: the exact JSON path Calendly uses to carry the
Facebook/YouTube/TikTok channel tag (tracking.utm_campaign vs. a custom
question/answer vs. something else) was not confirmed against a real
sample payload from the requirements doc at the time this was written,
and is now confirmed NOT to match real traffic (see REVISED note above -
this is exactly what caused the data loss). _extract_channel() below still
checks the most likely locations and logs a warning if none matches, but
no longer gates the write. Now that raw events are landing regardless,
inspect a real payload and fix this function (and its twin in the Glue
transform script) against real data.

Environment variables (set by the deploying stack, never hardcoded):
  SIGNING_SECRET_ARN           - Secrets Manager ARN of the Calendly signing secret
  BUCKET_NAME                  - InsightFlow data bucket
  REQUIRE_SIGNATURE_VALIDATION - "true" or "false" (default "false" as of
      Aug 12, 2026 - see below). Calendly's signing key is an OPTIONAL
      field the subscription creator can choose to set or leave blank -
      if left blank, no signature is ever sent at all, and there's nothing
      to hand over, ever. SME confirmed no signature is used on the
      CRM/Close endpoint specifically; Calendly was never separately
      confirmed. Defaulting to "false" here is a project decision made
      given that pattern and Calendly's optional-signature mechanism, not
      an SME answer - flip back to "true" if that assumption turns out
      wrong.
"""

import base64
import hashlib
import hmac
import json
import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

BUCKET_NAME = os.environ.get("BUCKET_NAME")
SIGNING_SECRET_ARN = os.environ.get("SIGNING_SECRET_ARN")
REQUIRE_SIGNATURE_VALIDATION = os.environ.get("REQUIRE_SIGNATURE_VALIDATION", "true").lower() != "false"

# The three channels this pipeline tracks, per Section 2/7 of the design doc.
TRACKED_EVENT_TYPES = {"facebook_paid_ads", "youtube_paid_ads", "tiktok_paid_ads"}

_cached_signing_key = None


def _get_signing_key() -> str:
    """Fetch and cache the Calendly webhook signing secret."""
    global _cached_signing_key
    if _cached_signing_key is not None:
        return _cached_signing_key

    try:
        response = secrets_client.get_secret_value(SecretId=SIGNING_SECRET_ARN)
    except ClientError:
        logger.exception("Failed to retrieve Calendly signing secret from Secrets Manager")
        raise

    _cached_signing_key = response["SecretString"]
    return _cached_signing_key


def _get_header(headers: dict, name: str) -> str:
    """Case-insensitive header lookup - API Gateway integration type affects casing."""
    if not headers:
        return None
    target = name.lower()
    for key, value in headers.items():
        if key.lower() == target:
            return value
    return None


def _verify_signature(signature_header: str, raw_body: str) -> bool:
    """
    Reproduces Calendly's documented verification:
      header format: "t=<timestamp>,v1=<hex_signature>"
      signed_payload = f"{timestamp}.{raw_body}"
      expected = hmac.new(secret, signed_payload, sha256).hexdigest()
    """
    if not (signature_header and raw_body):
        return False

    try:
        parts = dict(p.split("=", 1) for p in signature_header.split(","))
        timestamp = parts["t"]
        provided_signature = parts["v1"]
    except (KeyError, ValueError):
        logger.warning("Malformed Calendly-Webhook-Signature header")
        return False

    signing_key = _get_signing_key()
    signed_payload = f"{timestamp}.{raw_body}".encode("utf-8")
    expected_signature = hmac.new(
        signing_key.encode("utf-8"), signed_payload, hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(expected_signature, provided_signature)


def _extract_invitee_id(event_body: dict) -> str:
    """Invitee UUID lives in payload.uri (a full URL) or payload.id, depending
    on API version. Prefer the plain id field; fall back to parsing the URI."""
    payload = event_body.get("payload", {})
    if payload.get("id"):
        return payload["id"]
    uri = payload.get("uri", "")
    return uri.rstrip("/").split("/")[-1] if uri else None


def _extract_channel(event_body: dict) -> str:
    """
    See module docstring's ASSUMPTION FLAGGED note. Checks, in order:
      1. payload.tracking.utm_campaign
      2. payload.tracking.utm_source
      3. A custom question/answer whose question text contains "channel" or "source"
    Returns None (not KeyError) if nothing matches, so the caller can log
    and acknowledge-without-processing rather than crash.
    """
    payload = event_body.get("payload", {})
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


def lambda_handler(event, context):
    headers = event.get("headers") or {}
    signature_header = _get_header(headers, "calendly-webhook-signature")

    raw_body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        raw_body = base64.b64decode(raw_body).decode("utf-8")

    if REQUIRE_SIGNATURE_VALIDATION:
        if not _verify_signature(signature_header, raw_body):
            logger.warning("Signature validation failed - rejecting request")
            return {"statusCode": 401, "body": json.dumps({"error": "invalid signature"})}
    else:
        # TEMPORARY - see REQUIRE_SIGNATURE_VALIDATION note in module
        # docstring. Deliberately noisy (WARNING, not INFO) so this can't
        # quietly stay disabled.
        logger.warning(
            "Signature validation is DISABLED (REQUIRE_SIGNATURE_VALIDATION=false) - "
            "accepting request unvalidated. This is a temporary bridge, not a permanent state."
        )

    try:
        event_body = json.loads(raw_body)
    except json.JSONDecodeError:
        logger.error("Request body was not valid JSON")
        return {"statusCode": 400, "body": json.dumps({"error": "invalid JSON payload"})}

    channel = _extract_channel(event_body)
    if channel is None:
        # NO LONGER a reason to skip writing raw - see REVISED note in
        # module docstring. Channel is still logged and included in the
        # eventual raw JSON body itself (whatever Calendly actually sent),
        # but classification decisions belong in the Glue transform layer,
        # not at ingestion - raw must stay a complete, unfiltered mirror.
        logger.warning(
            "Could not determine channel for event %s - writing to raw/ anyway; "
            "channel classification happens at transform time, not ingestion",
            event_body.get("event"),
        )
    elif channel not in TRACKED_EVENT_TYPES:
        logger.info("Event channel %s is not in the tracked set - writing to raw/ anyway", channel)

    invitee_id = _extract_invitee_id(event_body)
    if not invitee_id:
        logger.warning("No invitee id found on event - acknowledging without processing")
        return {"statusCode": 200, "body": json.dumps({"status": "ignored", "reason": "no invitee id"})}

    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_key = f"raw/calendly_webhook_events/dt={ingestion_date}/invitee_{invitee_id}.json"

    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=raw_key,
            Body=raw_body.encode("utf-8"),
            ContentType="application/json",
        )
    except ClientError:
        logger.exception("Failed to write raw event to S3 for invitee_id=%s", invitee_id)
        return {"statusCode": 500, "body": json.dumps({"error": "failed to persist event"})}

    logger.info("Processed Calendly webhook for invitee_id=%s channel=%s", invitee_id, channel)
    return {"statusCode": 200, "body": json.dumps({"status": "accepted", "invitee_id": invitee_id, "channel": channel})}