"""
Local sanity tests for lambda_function.py - no real AWS/network calls.

Run with: python3 -m pytest test_lambda_function.py -v
"""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

import lambda_function as lf

EXPECTED_DAY = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def _not_found_error():
    return ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")


@patch.object(lf, "s3_client")
def test_already_ingested_is_noop(mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    mock_s3.head_object.return_value = {}  # exists, no exception

    result = lf.lambda_handler({}, None)

    assert result == {"status": "already_ingested", "day": EXPECTED_DAY}
    mock_s3.put_object.assert_not_called()
    print("PASS: already-ingested day -> no-op")


@patch("lambda_function.urllib.request.urlopen")
@patch.object(lf, "s3_client")
def test_file_not_yet_available_is_noop(mock_s3, mock_urlopen):
    lf.BUCKET_NAME = "test-bucket"
    mock_s3.head_object.side_effect = _not_found_error()

    index_response = MagicMock()
    index_response.read.return_value = json.dumps({"files": ["spend_data_2020-01-01.json"]}).encode("utf-8")
    index_response.__enter__.return_value = index_response
    mock_urlopen.return_value = index_response

    result = lf.lambda_handler({}, None)

    assert result == {"status": "not_yet_available", "day": EXPECTED_DAY}
    mock_s3.put_object.assert_not_called()
    print("PASS: file not in index -> no-op, no write")


@patch("lambda_function.urllib.request.urlopen")
@patch.object(lf, "s3_client")
def test_file_available_gets_pulled_and_written(mock_s3, mock_urlopen):
    lf.BUCKET_NAME = "test-bucket"
    mock_s3.head_object.side_effect = _not_found_error()

    expected_filename = f"spend_data_{EXPECTED_DAY}.json"
    index_response = MagicMock()
    index_response.read.return_value = json.dumps({"files": [expected_filename]}).encode("utf-8")
    index_response.__enter__.return_value = index_response

    spend_content = json.dumps({"channel": "facebook_paid_ads", "spend": 123.45}).encode("utf-8")
    file_response = MagicMock()
    file_response.read.return_value = spend_content
    file_response.__enter__.return_value = file_response

    mock_urlopen.side_effect = [index_response, file_response]

    result = lf.lambda_handler({}, None)

    assert result == {"status": "ingested", "day": EXPECTED_DAY}
    put_call = mock_s3.put_object.call_args
    assert put_call.kwargs["Bucket"] == "test-bucket"
    assert put_call.kwargs["Key"] == f"raw/calendly_spend/dt={EXPECTED_DAY}/spend_data_{EXPECTED_DAY}.json"
    assert put_call.kwargs["Body"] == spend_content  # unmodified raw mirror
    print("PASS: file available -> pulled and written unmodified to raw/")


@patch("lambda_function.urllib.request.urlopen")
@patch.object(lf, "s3_client")
def test_index_list_format_also_supported(mock_s3, mock_urlopen):
    """_is_file_available handles both a bare list and a {"files": [...]}
    dict, per the ASSUMPTION FLAGGED note - this covers the bare-list case."""
    lf.BUCKET_NAME = "test-bucket"
    mock_s3.head_object.side_effect = _not_found_error()

    expected_filename = f"spend_data_{EXPECTED_DAY}.json"
    index_response = MagicMock()
    index_response.read.return_value = json.dumps([expected_filename]).encode("utf-8")  # bare list
    index_response.__enter__.return_value = index_response

    file_response = MagicMock()
    file_response.read.return_value = b'{"spend": 1}'
    file_response.__enter__.return_value = file_response

    mock_urlopen.side_effect = [index_response, file_response]

    result = lf.lambda_handler({}, None)

    assert result["status"] == "ingested"
    print("PASS: bare-list file_index.json format also handled")


@patch("lambda_function.urllib.request.urlopen")
@patch.object(lf, "s3_client")
def test_index_fetch_failure_treated_as_not_available(mock_s3, mock_urlopen):
    """A network hiccup reading file_index.json must not crash the run -
    just means 'try again next scheduled invocation'."""
    lf.BUCKET_NAME = "test-bucket"
    mock_s3.head_object.side_effect = _not_found_error()
    mock_urlopen.side_effect = TimeoutError("simulated timeout")

    result = lf.lambda_handler({}, None)

    assert result["status"] == "not_yet_available"
    mock_s3.put_object.assert_not_called()
    print("PASS: index fetch failure -> graceful not-yet-available, not a crash")


if __name__ == "__main__":
    test_already_ingested_is_noop()
    test_file_not_yet_available_is_noop()
    test_file_available_gets_pulled_and_written()
    test_index_list_format_also_supported()
    test_index_fetch_failure_treated_as_not_available()
    print("\nAll tests passed.")
