"""
InsightFlow - CRM (Close) consumer handler.

Triggered by the SQS delay queue (event source mapping) once the 10-minute
delay set by crm_webhook_handler elapses. Per Section 6 of the InsightFlow
Solution Design doc:

  1. Read the raw webhook event back from S3 (written by crm_webhook_handler).
  2. Look up the lead owner from the public dea-lead-owner bucket by lead_id.
     That bucket holds Day-1 (T-1) data (SME-confirmed) - a missing or stale
     owner on a brand-new lead is a valid, expected outcome, not an error.
  3. Merge: the original webhook event is authoritative for lead-creation
     facts (display_name, status_label, date_created); the lookup only
     contributes lead_owner and lead_email, when present.
  4. Idempotency: if processed/crm_leads_enriched/{lead_id}.parquet already
     exists, this is a no-op (SQS is at-least-once delivery, so redelivery
     is expected, not exceptional).
  5. Write the merged record as Parquet (matches the format Calendly/Wistia
     Glue outputs use, so all three processed/ tables catalog uniformly).
  6. Post a Slack notification.

LEAD_OWNER_BASE_URL default is confirmed directly from the requirements doc's
own formula: public_url = f"https://{bucket_name}.s3.us-east-1.amazonaws.com/
{file_name}" - the earlier default here was missing the region in the
hostname (dea-lead-owner.s3.amazonaws.com instead of
dea-lead-owner.s3.us-east-1.amazonaws.com), which would have caused every
lookup to fail or redirect unexpectedly. Still kept as an env var rather
than inlined, for the same reason every other external endpoint in this
project is - easy to repoint without a code change if it ever needs to.

Environment variables:
  BUCKET_NAME          - InsightFlow data bucket
  LEAD_OWNER_BASE_URL  - base URL for the public lookup, lead_id + ".json" is appended
  SLACK_WEBHOOK_SECRET_ARN - Secrets Manager ARN holding the Slack incoming webhook URL
"""

import io
import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

import awswrangler as wr
import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

BUCKET_NAME = os.environ.get("BUCKET_NAME")
LEAD_OWNER_BASE_URL = os.environ.get(
    "LEAD_OWNER_BASE_URL", "https://dea-lead-owner.s3.us-east-1.amazonaws.com"
)
SLACK_WEBHOOK_SECRET_ARN = os.environ.get("SLACK_WEBHOOK_SECRET_ARN")

_cached_slack_webhook_url = None


def _get_slack_webhook_url() -> str:
    global _cached_slack_webhook_url
    if _cached_slack_webhook_url is not None:
        return _cached_slack_webhook_url

    try:
        response = secrets_client.get_secret_value(SecretId=SLACK_WEBHOOK_SECRET_ARN)
    except ClientError:
        logger.exception("Failed to retrieve Slack webhook URL from Secrets Manager")
        raise

    _cached_slack_webhook_url = response["SecretString"]
    return _cached_slack_webhook_url


def _already_processed(lead_id: str) -> bool:
    """Idempotency check - SQS is at-least-once, so this WILL be called
    more than once for some leads. That's expected, not a bug.

    NOTE: HeadObject on a genuinely missing key returns 404 only if the
    caller also has s3:ListBucket on the bucket (scoped via a Condition to
    this prefix, see foundation.yaml's ListBucketForIdempotencyCheck).
    Without it, S3 deliberately masks "not found" as 403 Forbidden, to
    avoid revealing object existence to a principal without list rights.
    This is a real S3 behavior, not a code bug - caught in testing when
    this check failed with 403 on an object that legitimately didn't exist
    yet, on a role that had GetObject/PutObject but no ListBucket."""
    key = f"processed/crm_leads_enriched/lead_{lead_id}.parquet"
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def _read_raw_event(raw_s3_key: str) -> dict:
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=raw_s3_key)
    return json.loads(response["Body"].read().decode("utf-8"))


def _lookup_lead_owner(lead_id: str) -> dict:
    """
    Anonymous public HTTPS GET - no IAM/boto3 needed, the bucket is public.
    A missing file (404) or a file with a null lead_owner is a VALID
    outcome given the T-1 refresh cycle, not an error - see module
    docstring. Any other failure (network, 5xx, malformed JSON) is logged
    but still treated as "no owner data available" rather than failing the
    whole lead - a notification with a pending owner is more useful than a
    lead silently never processed at all.
    """
    url = f"{LEAD_OWNER_BASE_URL}/{lead_id}.json"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.info("No lead-owner file yet for lead_id=%s (expected, T-1 data)", lead_id)
        else:
            logger.warning("Lead-owner lookup returned HTTP %s for lead_id=%s", e.code, lead_id)
        return {}
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError):
        logger.warning("Lead-owner lookup failed for lead_id=%s", lead_id, exc_info=True)
        return {}


def _merge(webhook_event: dict, owner_data: dict, lead_id: str) -> dict:
    """Webhook wins on overlapping fields; lookup only contributes
    lead_owner and lead_email, per Section 6's documented precedence."""
    envelope = webhook_event.get("event", {})
    lead_data = envelope.get("data", {}) if envelope.get("object_type") == "lead" else {}

    return {
        "lead_id": lead_id,
        "display_name": lead_data.get("display_name"),
        "status_label": lead_data.get("status_label"),
        "date_created": lead_data.get("date_created"),
        "lead_owner": owner_data.get("lead_owner"),
        "lead_email": owner_data.get("lead_email"),
        "funnel": owner_data.get("funnel"),
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


def _write_parquet(record: dict, lead_id: str) -> str:
    df = pd.DataFrame([record])
    key = f"processed/crm_leads_enriched/lead_{lead_id}.parquet"
    wr.s3.to_parquet(df=df, path=f"s3://{BUCKET_NAME}/{key}", dataset=False)
    return key


def _notify_slack(record: dict) -> None:
    webhook_url = _get_slack_webhook_url()
    owner_display = record.get("lead_owner") or "pending (not yet assigned)"
    text = (
        f"*New CRM Lead*\n"
        f"Name: {record.get('display_name')}\n"
        f"Lead ID: {record.get('lead_id')}\n"
        f"Created: {record.get('date_created')}\n"
        f"Status: {record.get('status_label')}\n"
        f"Email: {record.get('lead_email') or 'pending'}\n"
        f"Owner: {owner_display}\n"
        f"Funnel: {record.get('funnel') or 'unknown'}"
    )
    payload = json.dumps({"text": text}).encode("utf-8")
    try:
        req = urllib.request.Request(
            webhook_url, data=payload, headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        # Broad by design: a malformed/placeholder webhook URL (ValueError),
        # a network failure (URLError), a timeout, or anything else Slack-
        # related must never fail message processing - the data is already
        # durably written by this point (_write_parquet ran first). Losing
        # a notification is recoverable by checking S3 directly; retrying
        # and DLQ'ing an already-successful write due to a Slack hiccup is
        # a worse outcome and pollutes the DLQ alarm with false positives.
        # Slack failing shouldn't fail the whole message processing - the
        # data is already durably written by this point. Log and move on.
        logger.warning("Slack notification failed for lead_id=%s", record.get("lead_id"), exc_info=True)


def _process_one_message(body: dict) -> None:
    lead_id = body["lead_id"]
    raw_s3_key = body["raw_s3_key"]

    if _already_processed(lead_id):
        logger.info("lead_id=%s already processed - no-op (idempotent redelivery)", lead_id)
        return

    webhook_event = _read_raw_event(raw_s3_key)
    owner_data = _lookup_lead_owner(lead_id)
    record = _merge(webhook_event, owner_data, lead_id)
    _write_parquet(record, lead_id)
    _notify_slack(record)
    logger.info("Processed lead_id=%s (owner=%s)", lead_id, record.get("lead_owner") or "pending")


def lambda_handler(event, context):
    """
    SQS event source mapping delivers a batch of records. Any message that
    raises is left un-deleted by Lambda's SQS integration (returned via
    batchItemFailures), so it's retried and eventually DLQ'd per the
    queue's redrive policy - no manual retry logic needed here.
    """
    batch_item_failures = []

    for record in event.get("Records", []):
        message_id = record["messageId"]
        try:
            body = json.loads(record["body"])
            _process_one_message(body)
        except Exception:
            logger.exception("Failed to process SQS message %s", message_id)
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
