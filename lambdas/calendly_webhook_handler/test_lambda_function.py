"""
Local sanity tests for lambda_function.py - no real AWS calls.

Run with: python3 -m pytest test_lambda_function.py -v
"""
import hashlib
import hmac
import json
from unittest.mock import patch

import lambda_function as lf

SIGNING_KEY = "whsec_test_signing_key_1234567890"


def _sign(timestamp: str, body: str) -> str:
    signed_payload = f"{timestamp}.{body}".encode("utf-8")
    signature = hmac.new(SIGNING_KEY.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={signature}"


def _invitee_created_body(invitee_id="INV123", channel="facebook_paid_ads"):
    return json.dumps({
        "event": "invitee.created",
        "payload": {
            "id": invitee_id,
            "uri": f"https://api.calendly.com/scheduled_events/EVT1/invitees/{invitee_id}",
            "name": "Chris Garzon",
            "email": "chrisgarzon19@gmail.com",
            "tracking": {
                "utm_campaign": channel,
                "utm_source": "meta",
            },
            "questions_and_answers": [],
        },
    })


def _api_gw_event(body: str, sig_header: str):
    return {
        "headers": {
            "calendly-webhook-signature": sig_header,
            "Content-Type": "application/json",
        },
        "body": body,
        "isBase64Encoded": False,
    }


@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY)
def test_valid_tracked_channel_writes_s3(mock_key, mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    body = _invitee_created_body(invitee_id="INV123", channel="youtube_paid_ads")
    timestamp = "1700000000"
    sig = _sign(timestamp, body)
    event = _api_gw_event(body, sig)

    result = lf.lambda_handler(event, None)

    assert result["statusCode"] == 200
    payload = json.loads(result["body"])
    assert payload["status"] == "accepted"
    assert payload["invitee_id"] == "INV123"
    assert payload["channel"] == "youtube_paid_ads"

    put_call = mock_s3.put_object.call_args
    assert put_call.kwargs["Bucket"] == "test-bucket"
    assert "raw/calendly_webhook_events/dt=" in put_call.kwargs["Key"]
    assert "invitee_INV123.json" in put_call.kwargs["Key"]
    assert put_call.kwargs["Body"] == body.encode("utf-8")
    print("PASS: valid signature + tracked channel -> S3 write")


@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY)
def test_invalid_signature_rejected(mock_key, mock_s3):
    body = _invitee_created_body()
    tampered_sig = "t=1700000000,v1=" + ("0" * 64)
    event = _api_gw_event(body, tampered_sig)

    result = lf.lambda_handler(event, None)

    assert result["statusCode"] == 401
    mock_s3.put_object.assert_not_called()
    print("PASS: invalid signature -> 401, no S3 write")


@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY)
def test_toggle_disabled_accepts_bad_signature(mock_key, mock_s3):
    """REQUIRE_SIGNATURE_VALIDATION escape hatch, same rationale as the
    CRM webhook's identical test. Also covers Calendly's specific wrinkle:
    an empty/missing signature header (the case where no signing_key was
    ever set on the subscription) must ALSO be accepted when disabled."""
    original = lf.REQUIRE_SIGNATURE_VALIDATION
    lf.REQUIRE_SIGNATURE_VALIDATION = False
    try:
        body = _invitee_created_body(channel="facebook_paid_ads")
        event = _api_gw_event(body, "")  # no signature header at all

        result = lf.lambda_handler(event, None)

        assert result["statusCode"] == 200
        mock_s3.put_object.assert_called_once()
        print("PASS: REQUIRE_SIGNATURE_VALIDATION=false -> missing/bad signature accepted")
    finally:
        lf.REQUIRE_SIGNATURE_VALIDATION = original


@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY)
def test_untracked_channel_still_written_to_raw(mock_key, mock_s3):
    """REVISED behavior (Aug 14, 2026): an untracked/unrecognized channel
    must NOT prevent the write - this is the exact regression that lost
    real production Calendly bookings when channel filtering used to gate
    the write. Classification now happens at transform time, not here."""
    body = _invitee_created_body(channel="organic_search")  # not in TRACKED_EVENT_TYPES
    timestamp = "1700000000"
    sig = _sign(timestamp, body)
    event = _api_gw_event(body, sig)

    result = lf.lambda_handler(event, None)

    assert result["statusCode"] == 200
    payload = json.loads(result["body"])
    assert payload["status"] == "accepted"  # not "ignored" - this is the actual fix being tested
    mock_s3.put_object.assert_called_once()
    print("PASS: untracked channel -> still written to raw/, not silently discarded")


@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY)
def test_unrecognized_channel_shape_still_written_to_raw(mock_key, mock_s3):
    """The specific real-world failure: a payload whose tracking fields
    don't match ANY of _extract_channel's guessed locations at all (not
    just an untracked value, but no match whatsoever - channel comes back
    None). This is exactly what happened against real Calendly traffic.
    Must still write to raw/ with channel=null, not discard the event."""
    body = json.dumps({
        "event": "invitee.created",
        "payload": {
            "id": "INV_UNKNOWN_SHAPE",
            "tracking": {"utm_campaign": None, "utm_source": None},
            "questions_and_answers": [],
        },
    })
    timestamp = "1700000000"
    sig = _sign(timestamp, body)
    event = _api_gw_event(body, sig)

    result = lf.lambda_handler(event, None)

    assert result["statusCode"] == 200
    payload = json.loads(result["body"])
    assert payload["status"] == "accepted"
    assert payload["channel"] is None
    mock_s3.put_object.assert_called_once()
    print("PASS: unrecognized channel shape (channel=None) -> still written to raw/, not lost")


@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY)
def test_malformed_signature_header_rejected(mock_key, mock_s3):
    body = _invitee_created_body()
    event = _api_gw_event(body, "not-a-valid-header-format")

    result = lf.lambda_handler(event, None)

    assert result["statusCode"] == 401
    mock_s3.put_object.assert_not_called()
    print("PASS: malformed signature header -> 401")


@patch.object(lf, "s3_client")
@patch.object(lf, "_get_signing_key", return_value=SIGNING_KEY)
def test_channel_from_custom_question_fallback(mock_key, mock_s3):
    # No tracking.utm_campaign match - channel only identifiable via a
    # custom Q&A pair, exercising the fallback path in _extract_channel.
    body = json.dumps({
        "event": "invitee.created",
        "payload": {
            "id": "INV999",
            "tracking": {"utm_campaign": None, "utm_source": None},
            "questions_and_answers": [
                {"question": "How did you hear about us? (channel)", "answer": "tiktok_paid_ads"}
            ],
        },
    })
    timestamp = "1700000000"
    sig = _sign(timestamp, body)
    event = _api_gw_event(body, sig)

    result = lf.lambda_handler(event, None)

    assert result["statusCode"] == 200
    assert json.loads(result["body"])["channel"] == "tiktok_paid_ads"
    print("PASS: channel resolved via custom-question fallback")


if __name__ == "__main__":
    test_valid_tracked_channel_writes_s3()
    test_invalid_signature_rejected()
    test_toggle_disabled_accepts_bad_signature()
    test_untracked_channel_still_written_to_raw()
    test_unrecognized_channel_shape_still_written_to_raw()
    test_malformed_signature_header_rejected()
    test_channel_from_custom_question_fallback()
    print("\nAll tests passed.")