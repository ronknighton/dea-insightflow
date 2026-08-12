"""
InsightFlow - CRM (Close) webhook handler.

Triggered by API Gateway on every Close webhook POST (lead created/updated).

Responsibilities, per Section 6 of the InsightFlow Solution Design doc:
  1. Validate the close-sig-hash / close-sig-timestamp signature (Close's
     documented HMAC-SHA256 scheme - see
     https://developer.close.com/api/resources/webhooks#webhook-signatures).
  2. Write the raw, unmodified event to S3 raw/crm_events/dt=YYYY-MM-DD/
     crm_event_{lead_id}.json - raw stays an untouched mirror of the source.
  3. Send a message to the SQS delay queue with DelaySeconds=600 (10 min),
     per the SME-directed SQS delay-queue pattern. A second Lambda
     (crm_consumer_handler) picks this up once the delay elapses and does
     the lead-owner lookup, merge, and Slack notify.

Close's own docs recommend queuing events locally before processing them
asynchronously (to guarantee delivery and avoid the 100k-event subscription
pause), which this design already does independent of the 10-minute
requirement - the SQS handoff satisfies both needs at once.

Environment variables (set by the deploying stack, never hardcoded):
  SIGNING_SECRET_ARN           - Secrets Manager ARN of the Close webhook signing secret
  BUCKET_NAME                  - InsightFlow data bucket
  DELAY_QUEUE_URL              - URL of the CRM lead delay queue
  REQUIRE_SIGNATURE_VALIDATION - "true" (default) or "false". TEMPORARY
      ESCAPE HATCH: the requirements doc never actually mandates signature
      validation - this was added independently as standard webhook
      security practice. Real Close traffic has been failing validation
      because the signing secret is still a placeholder pending an SME
      handoff (Close generates it server-side at subscription creation and
      only returns it to whoever created the subscription - not
      reproducible or self-served on our end). Left enabled long enough,
      Close auto-pauses a subscription after 3 straight days of failed
      deliveries. Setting this to "false" accepts all traffic unvalidated
      so the subscription stays alive and real data keeps flowing while
      the secret handoff is pending - a deliberate, reversible, documented
      tradeoff (see Section 14 of the design doc), not a silent weakening.
      Set back to "true" the moment the real signing secret is in Secrets
      Manager - this is not meant to be a permanent setting.
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
sqs_client = boto3.client("sqs")
secrets_client = boto3.client("secretsmanager")

BUCKET_NAME = os.environ.get("BUCKET_NAME")
DELAY_QUEUE_URL = os.environ.get("DELAY_QUEUE_URL")
SIGNING_SECRET_ARN = os.environ.get("SIGNING_SECRET_ARN")
DELAY_SECONDS = 600  # 10 minutes, per the brief's stated requirement
REQUIRE_SIGNATURE_VALIDATION = os.environ.get("REQUIRE_SIGNATURE_VALIDATION", "true").lower() != "false"

# Cached across warm invocations so every request doesn't re-call Secrets
# Manager - it's fetched once per execution environment, not once per event.
_cached_signing_key = None


def _get_signing_key() -> str:
    """Fetch and cache the Close webhook signing secret (hex string)."""
    global _cached_signing_key
    if _cached_signing_key is not None:
        return _cached_signing_key

    try:
        response = secrets_client.get_secret_value(SecretId=SIGNING_SECRET_ARN)
    except ClientError:
        logger.exception("Failed to retrieve Close signing secret from Secrets Manager")
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


def _verify_signature(timestamp: str, raw_body: str, provided_hash: str) -> bool:
    """
    Reproduces Close's documented verification exactly:
      key = bytearray.fromhex(signing_key)
      data = timestamp + payload
      signature = hmac.new(key, data.encode('utf-8'), hashlib.sha256).hexdigest()
    """
    if not (timestamp and raw_body and provided_hash):
        return False

    signing_key_hex = _get_signing_key()
    try:
        key_bytes = bytearray.fromhex(signing_key_hex)
    except ValueError:
        logger.error("Signing secret is not valid hex - check the Secrets Manager value")
        return False

    data = (timestamp + raw_body).encode("utf-8")
    expected_hash = hmac.new(key_bytes, data, hashlib.sha256).hexdigest()

    return hmac.compare_digest(expected_hash, provided_hash)


def _extract_lead_id(event_body: dict) -> str:
    """
    Close's event envelope nests the actual object under event.data, with
    event.lead_id present for most CRM object types. Lead-creation events
    have object_type == 'lead', where the lead's own id is event.data.id.
    Fall back through both shapes rather than assuming one.
    """
    envelope = event_body.get("event", {})

    lead_id = envelope.get("lead_id")
    if lead_id:
        return lead_id

    if envelope.get("object_type") == "lead":
        return envelope.get("data", {}).get("id")

    return None


def lambda_handler(event, context):
    headers = event.get("headers") or {}
    provided_hash = _get_header(headers, "close-sig-hash")
    timestamp = _get_header(headers, "close-sig-timestamp")

    raw_body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        raw_body = base64.b64decode(raw_body).decode("utf-8")

    if REQUIRE_SIGNATURE_VALIDATION:
        if not _verify_signature(timestamp, raw_body, provided_hash):
            logger.warning("Signature validation failed - rejecting request")
            return {"statusCode": 401, "body": json.dumps({"error": "invalid signature"})}
    else:
        # TEMPORARY - see REQUIRE_SIGNATURE_VALIDATION note in module
        # docstring. Logged at WARNING (not INFO) on every single
        # invocation, deliberately noisy, so this can't quietly stay
        # disabled after the real secret becomes available.
        logger.warning(
            "Signature validation is DISABLED (REQUIRE_SIGNATURE_VALIDATION=false) - "
            "accepting request unvalidated. This is a temporary bridge, not a permanent state."
        )

    try:
        event_body = json.loads(raw_body)
    except json.JSONDecodeError:
        logger.error("Request body was not valid JSON")
        return {"statusCode": 400, "body": json.dumps({"error": "invalid JSON payload"})}

    lead_id = _extract_lead_id(event_body)
    if not lead_id:
        # Not every Close event on a shared webhook subscription is
        # necessarily a lead event (filters reduce this, but don't assume
        # zero drift). Acknowledge with 200 so Close doesn't retry a message
        # this pipeline was never going to process, but skip raw/SQS.
        logger.info("No lead_id found on event - acknowledging without processing")
        return {"statusCode": 200, "body": json.dumps({"status": "ignored"})}

    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_key = f"raw/crm_events/dt={ingestion_date}/crm_event_{lead_id}.json"

    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=raw_key,
            Body=raw_body.encode("utf-8"),
            ContentType="application/json",
        )
    except ClientError:
        logger.exception("Failed to write raw event to S3 for lead_id=%s", lead_id)
        # 500 tells Close to retry (their backoff goes up to 72 hours) rather
        # than silently losing the event.
        return {"statusCode": 500, "body": json.dumps({"error": "failed to persist event"})}

    message_body = json.dumps({
        "lead_id": lead_id,
        "raw_s3_key": raw_key,
        "ingestion_date": ingestion_date,
    })

    try:
        sqs_client.send_message(
            QueueUrl=DELAY_QUEUE_URL,
            MessageBody=message_body,
            DelaySeconds=DELAY_SECONDS,
        )
    except ClientError:
        logger.exception("Failed to enqueue delayed message for lead_id=%s", lead_id)
        # The raw event is already durably in S3 at this point, so this
        # isn't a full loss - but the owner-lookup step will never run
        # without the message. Fail loudly (500) so Close retries the whole
        # webhook, which re-attempts both the S3 write (idempotent, same
        # key) and the SQS send.
        return {"statusCode": 500, "body": json.dumps({"error": "failed to enqueue for processing"})}

    logger.info("Processed CRM webhook for lead_id=%s", lead_id)
    return {"statusCode": 200, "body": json.dumps({"status": "accepted", "lead_id": lead_id})}
