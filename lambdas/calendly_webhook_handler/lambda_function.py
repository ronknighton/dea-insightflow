"""
InsightFlow - Calendly webhook handler.

Triggered by API Gateway on every Calendly webhook POST (invitee.created).

Responsibilities, per Section 7 of the InsightFlow Solution Design doc:
  1. Validate the Calendly-Webhook-Signature header (Calendly's documented
     HMAC-SHA256 scheme: t=<timestamp>,v1=<hex signature>, signed over
     "{timestamp}.{raw_body}" - see
     https://developer.calendly.com/api-docs/webhook-signatures).
  2. Filter to the three tracked paid-ad channels (Facebook, YouTube,
     TikTok). Untracked event types are acknowledged (200) but not written -
     mirrors the CRM webhook's "ignore, don't error" handling for events
     outside this pipeline's scope.
  3. Write the raw, unmodified event to S3
     raw/calendly_webhook_events/dt=YYYY-MM-DD/invitee_{invitee_id}.json.
     Unlike CRM, there is no delay/lookup step downstream - the join to
     spend data happens later in Glue/Athena, not per-event here.

ASSUMPTION FLAGGED: the exact JSON path Calendly uses to carry the
Facebook/YouTube/TikTok channel tag (tracking.utm_campaign vs. a custom
question/answer vs. something else) was not confirmed against a real
sample payload from the requirements doc at the time this was written.
_extract_channel() below checks the most likely locations and logs a
warning (without failing the request) if none matches, specifically so
this gap surfaces in CloudWatch on the first real webhook rather than
silently mis-filtering events. Verify against a live payload and tighten
this function before the 7-day operational window begins.

Environment variables (set by the deploying stack, never hardcoded):
  SIGNING_SECRET_ARN           - Secrets Manager ARN of the Calendly signing secret
  BUCKET_NAME                  - InsightFlow data bucket
  REQUIRE_SIGNATURE_VALIDATION - "true" (default) or "false". TEMPORARY
      ESCAPE HATCH - see the identical setting in crm_webhook_handler for
      full rationale. Calendly has an added wrinkle worth knowing: unlike
      Close, Calendly's signing key is an OPTIONAL field the subscription
      creator can choose to set or leave blank - if it was left blank, no
      signature is ever sent at all, and no secret value will ever arrive
      to fix that (there's nothing to hand over). Confirm with whoever
      created the subscription whether a signing_key was actually set
      before assuming this is just a pending handoff like the Close side.
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
        logger.warning(
            "Could not determine channel for event %s - see ASSUMPTION FLAGGED note "
            "in module docstring; acknowledging without processing",
            event_body.get("event"),
        )
        return {"statusCode": 200, "body": json.dumps({"status": "ignored", "reason": "unrecognized channel"})}

    if channel not in TRACKED_EVENT_TYPES:
        logger.info("Event channel %s is not tracked - acknowledging without processing", channel)
        return {"statusCode": 200, "body": json.dumps({"status": "ignored", "reason": "untracked channel"})}

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
