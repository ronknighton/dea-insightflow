"""
Local sanity tests for lambda_function.py - no real AWS calls. Mocks the
boto3 clients so this runs anywhere, including outside a deployed Lambda.

Run with: python3 -m pytest test_lambda_function.py -v
"""
import hashlib
import hmac
import json
from unittest.mock import MagicMock, patch

import lambda_function as lf

SIGNING_KEY_HEX = "058bfb6a3d8cfdc4da7c3be5901b16ae11da982b46a25fb2cd7016e97a140a1c"


def _sign(timestamp: str, body: str) -> str:
    """Reference implementation, matching Close's documented algorithm exactly."""
    key_bytes = bytearray.fromhex(SIGNING_KEY_HEX)
    data = (timestamp + body).encode("utf-8")
    return hmac.new(key_bytes, data, hashlib.sha256).hexdigest()


def _lead_created_body(lead_id="lead_zwqYhEFwzPyfCErS8uQ77is2wFLvr9BgVi6cTfbFM68"):
    return json.dumps({
        "event": {
            "id": "ev_2sYKRjcrA79yKxi3S4Crd7",
            "action": "created",
            "object_type": "lead",
            "date_created": "2026-08-10T12:48:23.395000",
            "data": {
                "id": lead_id,
                "display_name": "Acme Corp",
                "status_label": "Potential",
                "date_created": "2026-08-10T12:48:23.395000",
            },
        },
        "subscription_id": "whsub_8AmjKCZYT3zI8eZoi4HhFC",
    })


def _api_gw_event(body: str, timestamp: str, sig: str):
    return {
        "headers": {
            "close-sig-hash": sig,
            "close-sig-timestamp": timestamp,
            "Content-Type": "application/json",
        },
        "body": body,
        "isBase64Encoded": False,
    }


@patch.object(lf, "sqs_client")
@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY_HEX)
def test_valid_signature_writes_s3_and_enqueues_sqs(mock_key, mock_s3, mock_sqs):
    lf.BUCKET_NAME = "test-bucket"
    lf.DELAY_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"

    body = _lead_created_body()
    timestamp = "1544271440"
    sig = _sign(timestamp, body)
    event = _api_gw_event(body, timestamp, sig)

    result = lf.lambda_handler(event, None)

    assert result["statusCode"] == 200
    payload = json.loads(result["body"])
    assert payload["status"] == "accepted"
    assert payload["lead_id"] == "lead_zwqYhEFwzPyfCErS8uQ77is2wFLvr9BgVi6cTfbFM68"

    # S3 write happened, with the right key shape and unmodified body
    put_call = mock_s3.put_object.call_args
    assert put_call.kwargs["Bucket"] == "test-bucket"
    assert put_call.kwargs["Key"].startswith("raw/crm_events/dt=")
    assert "crm_event_lead_zwqYhEFwzPyfCErS8uQ77is2wFLvr9BgVi6cTfbFM68.json" in put_call.kwargs["Key"]
    assert put_call.kwargs["Body"] == body.encode("utf-8")  # raw stays unmodified

    # SQS send happened with the correct 10-minute delay
    send_call = mock_sqs.send_message.call_args
    assert send_call.kwargs["DelaySeconds"] == 600
    sent_body = json.loads(send_call.kwargs["MessageBody"])
    assert sent_body["lead_id"] == "lead_zwqYhEFwzPyfCErS8uQ77is2wFLvr9BgVi6cTfbFM68"
    print("PASS: valid signature -> S3 write + SQS enqueue with 600s delay")


@patch.object(lf, "sqs_client")
@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY_HEX)
def test_invalid_signature_rejected(mock_key, mock_s3, mock_sqs):
    body = _lead_created_body()
    timestamp = "1544271440"
    tampered_sig = "0" * 64  # definitely wrong
    event = _api_gw_event(body, timestamp, tampered_sig)

    result = lf.lambda_handler(event, None)

    assert result["statusCode"] == 401
    mock_s3.put_object.assert_not_called()
    mock_sqs.send_message.assert_not_called()
    print("PASS: invalid signature -> 401, no S3/SQS side effects")


@patch.object(lf, "sqs_client")
@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY_HEX)
def test_toggle_disabled_accepts_bad_signature(mock_key, mock_s3, mock_sqs):
    """The REQUIRE_SIGNATURE_VALIDATION escape hatch: with it set to false,
    even a deliberately wrong signature must be accepted and processed
    normally - this is the whole point of the toggle. Restores the module
    flag afterward so this test can't leak state into others."""
    original = lf.REQUIRE_SIGNATURE_VALIDATION
    lf.REQUIRE_SIGNATURE_VALIDATION = False
    try:
        body = _lead_created_body()
        timestamp = "1544271440"
        tampered_sig = "0" * 64  # would fail validation if it were checked
        event = _api_gw_event(body, timestamp, tampered_sig)

        result = lf.lambda_handler(event, None)

        assert result["statusCode"] == 200
        mock_s3.put_object.assert_called_once()
        mock_sqs.send_message.assert_called_once()
        print("PASS: REQUIRE_SIGNATURE_VALIDATION=false -> bad signature accepted and processed")
    finally:
        lf.REQUIRE_SIGNATURE_VALIDATION = original


@patch.object(lf, "sqs_client")
@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY_HEX)
def test_non_lead_event_acknowledged_but_skipped(mock_key, mock_s3, mock_sqs):
    # An opportunity event with no lead_id at the top level and object_type != lead
    body = json.dumps({
        "event": {
            "id": "ev_xyz",
            "action": "updated",
            "object_type": "opportunity",
            "data": {"id": "oppo_123", "status_label": "Won"},
        }
    })
    timestamp = "1544271440"
    sig = _sign(timestamp, body)
    event = _api_gw_event(body, timestamp, sig)

    result = lf.lambda_handler(event, None)

    assert result["statusCode"] == 200
    assert json.loads(result["body"])["status"] == "ignored"
    mock_s3.put_object.assert_not_called()
    mock_sqs.send_message.assert_not_called()
    print("PASS: non-lead event -> acknowledged, not processed")


@patch.object(lf, "sqs_client")
@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY_HEX)
def test_lead_id_at_event_top_level(mock_key, mock_s3, mock_sqs):
    # Mirrors the opportunity-won example from Close's own docs, where
    # lead_id sits at event.lead_id even though object_type is 'opportunity'
    body = json.dumps({
        "event": {
            "id": "ev_2sYKRjcrA79yKxi3S4Crd7",
            "action": "updated",
            "object_type": "opportunity",
            "lead_id": "lead_zwqYhEFwzPyfCErS8uQ77is2wFLvr9BgVi6cTfbFM68",
            "data": {"id": "oppo_7H4sjNso7FyBFaeR3RXi5PMJbilfo0c6UPCxsJtEhCO"},
        }
    })
    timestamp = "1544271440"
    sig = _sign(timestamp, body)
    event = _api_gw_event(body, timestamp, sig)

    result = lf.lambda_handler(event, None)

    assert result["statusCode"] == 200
    assert json.loads(result["body"])["lead_id"] == "lead_zwqYhEFwzPyfCErS8uQ77is2wFLvr9BgVi6cTfbFM68"
    print("PASS: event.lead_id fallback path works for non-lead object types")


if __name__ == "__main__":
    test_valid_signature_writes_s3_and_enqueues_sqs()
    test_invalid_signature_rejected()
    test_toggle_disabled_accepts_bad_signature()
    test_non_lead_event_acknowledged_but_skipped()
    test_lead_id_at_event_top_level()
    print("\nAll tests passed.")
