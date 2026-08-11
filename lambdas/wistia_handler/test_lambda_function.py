"""
Local sanity tests for lambda_function.py - no real AWS/network calls.

Run with: python3 -m pytest test_lambda_function.py -v
"""
import base64
import json
from unittest.mock import MagicMock, patch

import lambda_function as lf


def _mock_response(payload) -> MagicMock:
    resp = MagicMock()
    resp.read.return_value = json.dumps(payload).encode("utf-8")
    resp.__enter__.return_value = resp
    return resp


@patch.object(lf, "s3_client")
@patch.object(lf, "ssm_client")
@patch.object(lf, "_get_token", return_value="fake-token")
@patch("lambda_function.urllib.request.urlopen")
def test_uses_basic_auth_per_requirements_doc(mock_urlopen, mock_token, mock_ssm, mock_s3):
    """The requirements doc explicitly says Basic Auth, not Bearer - this
    locks that in so a future 'helpful' change back to Bearer breaks a
    test instead of silently shipping."""
    lf.BUCKET_NAME = "test-bucket"
    lf.MEDIA_IDS = ["media1"]
    mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "2026-08-10T12:00:00Z"}}

    mock_urlopen.side_effect = [
        _mock_response({"hashed_id": "media1", "name": "Test Video"}),  # metadata
        _mock_response({"engagement": 0.8}),  # engagement
        _mock_response([]),  # events
    ]

    lf.lambda_handler({}, None)

    first_call_request = mock_urlopen.call_args_list[0].args[0]
    auth_header = first_call_request.get_header("Authorization")

    assert auth_header.startswith("Basic ")
    decoded = base64.b64decode(auth_header.split(" ", 1)[1]).decode("utf-8")
    assert decoded == "api:fake-token"
    print("PASS: Authorization header is Basic auth with 'api' as username, matching the requirements doc")


@patch.object(lf, "s3_client")
@patch.object(lf, "ssm_client")
@patch.object(lf, "_get_token", return_value="fake-token")
@patch("lambda_function.urllib.request.urlopen")
def test_media_metadata_is_fetched_and_written(mock_urlopen, mock_token, mock_ssm, mock_s3):
    """Regression test for the gap found reviewing the requirements doc:
    media metadata is a separate, explicit extraction requirement from
    engagement metrics - this must be pulled and written every run."""
    lf.BUCKET_NAME = "test-bucket"
    lf.MEDIA_IDS = ["media1"]
    mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "2026-08-10T12:00:00Z"}}

    metadata_payload = {"hashed_id": "media1", "name": "Test Video", "created": "2026-01-01T00:00:00Z"}
    mock_urlopen.side_effect = [
        _mock_response(metadata_payload),
        _mock_response({"engagement": 0.8}),
        _mock_response([]),
    ]

    lf.lambda_handler({}, None)

    metadata_write = next(
        call for call in mock_s3.put_object.call_args_list
        if "/metadata.json" in call.kwargs["Key"]
    )
    written_body = json.loads(metadata_write.kwargs["Body"])
    assert written_body == metadata_payload
    print("PASS: media metadata is fetched and written to raw/ separately from engagement")


@patch.object(lf, "s3_client")
@patch.object(lf, "ssm_client")
@patch.object(lf, "_get_token", return_value="fake-token")
@patch("lambda_function.urllib.request.urlopen")
def test_first_run_no_watermark_pulls_all_and_sets_watermark(mock_urlopen, mock_token, mock_ssm, mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    lf.MEDIA_IDS = ["media1"]
    mock_ssm.exceptions.ParameterNotFound = type("ParameterNotFound", (Exception,), {})
    mock_ssm.get_parameter.side_effect = mock_ssm.exceptions.ParameterNotFound()

    mock_urlopen.side_effect = [
        _mock_response({"hashed_id": "media1"}),
        _mock_response({"engagement": 0.8}),
        _mock_response([
            {"received_at": "2026-08-10T12:00:00Z", "event_key": "e2"},
            {"received_at": "2026-08-10T11:00:00Z", "event_key": "e1"},
        ]),
    ]

    result = lf.lambda_handler({}, None)

    assert result["results"] == [{"media_id": "media1", "new_event_count": 2}]
    mock_ssm.put_parameter.assert_called_once()
    assert mock_ssm.put_parameter.call_args.kwargs["Value"] == "2026-08-10T12:00:00Z"
    print("PASS: first run (no watermark) -> pulls all events, sets watermark to newest")


@patch.object(lf, "s3_client")
@patch.object(lf, "ssm_client")
@patch.object(lf, "_get_token", return_value="fake-token")
@patch("lambda_function.urllib.request.urlopen")
def test_watermark_stops_pagination_correctly(mock_urlopen, mock_token, mock_ssm, mock_s3):
    """Core incremental-pull logic: events at or before the watermark must
    be excluded, and pagination must stop once the watermark is reached -
    not continue through all MAX_PAGES_PER_RUN pages."""
    lf.BUCKET_NAME = "test-bucket"
    lf.MEDIA_IDS = ["media1"]
    mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "2026-08-10T10:00:00Z"}}

    mock_urlopen.side_effect = [
        _mock_response({"hashed_id": "media1"}),
        _mock_response({"engagement": 0.8}),
        _mock_response([
            {"received_at": "2026-08-10T12:00:00Z", "event_key": "new2"},
            {"received_at": "2026-08-10T11:00:00Z", "event_key": "new1"},
            {"received_at": "2026-08-10T10:00:00Z", "event_key": "at_watermark"},
            {"received_at": "2026-08-10T09:00:00Z", "event_key": "old"},
        ]),
    ]

    result = lf.lambda_handler({}, None)

    assert result["results"] == [{"media_id": "media1", "new_event_count": 2}]
    assert mock_urlopen.call_count == 3  # metadata + engagement + events page 1 only
    print("PASS: watermark correctly excludes old events and halts pagination")


@patch.object(lf, "s3_client")
@patch.object(lf, "ssm_client")
@patch.object(lf, "_get_token", return_value="fake-token")
@patch("lambda_function.urllib.request.urlopen")
def test_no_new_events_does_not_write_or_update_watermark(mock_urlopen, mock_token, mock_ssm, mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    lf.MEDIA_IDS = ["media1"]
    mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "2026-08-10T12:00:00Z"}}

    mock_urlopen.side_effect = [
        _mock_response({"hashed_id": "media1"}),
        _mock_response({"engagement": 0.8}),
        _mock_response([{"received_at": "2026-08-10T12:00:00Z", "event_key": "at_watermark"}]),
    ]

    result = lf.lambda_handler({}, None)

    assert result["results"] == [{"media_id": "media1", "new_event_count": 0}]
    mock_ssm.put_parameter.assert_not_called()
    assert mock_s3.put_object.call_count == 2  # metadata + engagement only
    print("PASS: no new events -> no events file written, watermark untouched")


@patch.object(lf, "s3_client")
@patch.object(lf, "ssm_client")
@patch.object(lf, "_get_token", return_value="fake-token")
@patch("lambda_function.urllib.request.urlopen")
def test_one_media_failure_does_not_block_the_other(mock_urlopen, mock_token, mock_ssm, mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    lf.MEDIA_IDS = ["bad_media", "good_media"]
    mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "2026-08-10T12:00:00Z"}}

    mock_urlopen.side_effect = [
        RuntimeError("simulated API failure"),
        _mock_response({"hashed_id": "good_media"}),
        _mock_response({"engagement": 0.5}),
        _mock_response([]),
    ]

    result = lf.lambda_handler({}, None)

    assert result["results"][0] == {"media_id": "bad_media", "error": True}
    assert result["results"][1] == {"media_id": "good_media", "new_event_count": 0}
    print("PASS: one media's failure is isolated, doesn't block the other")


if __name__ == "__main__":
    test_uses_basic_auth_per_requirements_doc()
    test_media_metadata_is_fetched_and_written()
    test_first_run_no_watermark_pulls_all_and_sets_watermark()
    test_watermark_stops_pagination_correctly()
    test_no_new_events_does_not_write_or_update_watermark()
    test_one_media_failure_does_not_block_the_other()
    print("\nAll tests passed.")
