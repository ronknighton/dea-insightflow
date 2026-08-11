"""
Local sanity tests for lambda_function.py - no real AWS/network calls.

Run with: python3 -m pytest test_lambda_function.py -v
"""
import json
from unittest.mock import MagicMock, patch

import lambda_function as lf


def _mock_ssm_not_found():
    """SSM's exceptions live on the client instance, not the module -
    build a fake exception class + client shaped the way boto3 expects."""
    client = MagicMock()

    class ParameterNotFound(Exception):
        pass

    client.exceptions.ParameterNotFound = ParameterNotFound
    client.get_parameter.side_effect = ParameterNotFound()
    return client


@patch.object(lf, "s3_client")
@patch.object(lf, "ssm_client")
@patch.object(lf, "_get_token", return_value="fake-token")
@patch("lambda_function.urllib.request.urlopen")
def test_first_run_no_watermark_pulls_all_and_sets_watermark(mock_urlopen, mock_token, mock_ssm, mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    lf.MEDIA_IDS = ["media1"]
    mock_ssm.exceptions.ParameterNotFound = type("ParameterNotFound", (Exception,), {})
    mock_ssm.get_parameter.side_effect = mock_ssm.exceptions.ParameterNotFound()

    engagement_resp = MagicMock()
    engagement_resp.read.return_value = json.dumps({"engagement": 0.8}).encode("utf-8")
    engagement_resp.__enter__.return_value = engagement_resp

    events_page1 = MagicMock()
    events_page1.read.return_value = json.dumps([
        {"received_at": "2026-08-10T12:00:00Z", "event_key": "e2"},
        {"received_at": "2026-08-10T11:00:00Z", "event_key": "e1"},
    ]).encode("utf-8")
    events_page1.__enter__.return_value = events_page1

    mock_urlopen.side_effect = [engagement_resp, events_page1]

    result = lf.lambda_handler({}, None)

    assert result["results"] == [{"media_id": "media1", "new_event_count": 2}]
    mock_ssm.put_parameter.assert_called_once()
    put_kwargs = mock_ssm.put_parameter.call_args.kwargs
    assert put_kwargs["Value"] == "2026-08-10T12:00:00Z"  # newest event's timestamp
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

    engagement_resp = MagicMock()
    engagement_resp.read.return_value = json.dumps({"engagement": 0.8}).encode("utf-8")
    engagement_resp.__enter__.return_value = engagement_resp

    # Newest-first page: 2 new events, then one at exactly the watermark (excluded), then would-be-older
    events_page1 = MagicMock()
    events_page1.read.return_value = json.dumps([
        {"received_at": "2026-08-10T12:00:00Z", "event_key": "new2"},
        {"received_at": "2026-08-10T11:00:00Z", "event_key": "new1"},
        {"received_at": "2026-08-10T10:00:00Z", "event_key": "at_watermark"},  # excluded: <= watermark
        {"received_at": "2026-08-10T09:00:00Z", "event_key": "old"},  # should never be reached/counted
    ]).encode("utf-8")
    events_page1.__enter__.return_value = events_page1

    mock_urlopen.side_effect = [engagement_resp, events_page1]

    result = lf.lambda_handler({}, None)

    assert result["results"] == [{"media_id": "media1", "new_event_count": 2}]
    # Only one urlopen call for events (page 1) - confirms it stopped, didn't fetch page 2
    assert mock_urlopen.call_count == 2  # engagement + events page 1 only
    print("PASS: watermark correctly excludes old events and halts pagination")


@patch.object(lf, "s3_client")
@patch.object(lf, "ssm_client")
@patch.object(lf, "_get_token", return_value="fake-token")
@patch("lambda_function.urllib.request.urlopen")
def test_no_new_events_does_not_write_or_update_watermark(mock_urlopen, mock_token, mock_ssm, mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    lf.MEDIA_IDS = ["media1"]
    mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "2026-08-10T12:00:00Z"}}

    engagement_resp = MagicMock()
    engagement_resp.read.return_value = json.dumps({"engagement": 0.8}).encode("utf-8")
    engagement_resp.__enter__.return_value = engagement_resp

    events_page1 = MagicMock()
    events_page1.read.return_value = json.dumps([
        {"received_at": "2026-08-10T12:00:00Z", "event_key": "at_watermark"},  # excluded
    ]).encode("utf-8")
    events_page1.__enter__.return_value = events_page1

    mock_urlopen.side_effect = [engagement_resp, events_page1]

    result = lf.lambda_handler({}, None)

    assert result["results"] == [{"media_id": "media1", "new_event_count": 0}]
    mock_ssm.put_parameter.assert_not_called()
    # Only 1 S3 write (engagement snapshot) - no events file written since there were none new
    assert mock_s3.put_object.call_count == 1
    print("PASS: no new events -> no events file written, watermark untouched")


@patch.object(lf, "s3_client")
@patch.object(lf, "ssm_client")
@patch.object(lf, "_get_token", return_value="fake-token")
@patch("lambda_function.urllib.request.urlopen")
def test_one_media_failure_does_not_block_the_other(mock_urlopen, mock_token, mock_ssm, mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    lf.MEDIA_IDS = ["bad_media", "good_media"]
    mock_ssm.get_parameter.return_value = {"Parameter": {"Value": "2026-08-10T12:00:00Z"}}

    good_engagement = MagicMock()
    good_engagement.read.return_value = json.dumps({"engagement": 0.5}).encode("utf-8")
    good_engagement.__enter__.return_value = good_engagement
    good_events = MagicMock()
    good_events.read.return_value = json.dumps([]).encode("utf-8")
    good_events.__enter__.return_value = good_events

    # First media's engagement call raises; second media's two calls succeed
    mock_urlopen.side_effect = [RuntimeError("simulated API failure"), good_engagement, good_events]

    result = lf.lambda_handler({}, None)

    assert result["results"][0] == {"media_id": "bad_media", "error": True}
    assert result["results"][1] == {"media_id": "good_media", "new_event_count": 0}
    print("PASS: one media's failure is isolated, doesn't block the other")


if __name__ == "__main__":
    test_first_run_no_watermark_pulls_all_and_sets_watermark()
    test_watermark_stops_pagination_correctly()
    test_no_new_events_does_not_write_or_update_watermark()
    test_one_media_failure_does_not_block_the_other()
    print("\nAll tests passed.")
