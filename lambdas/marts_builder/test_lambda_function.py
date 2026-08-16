"""
Local sanity tests for lambda_function.py - no real AWS/Athena calls.

These test the REBUILD MECHANISM (drop -> delete -> CTAS sequence,
polling, error surfacing) - not whether the SQL in MART_QUERIES produces
correct business results, which per the module's own ASSUMPTION FLAGGED
notes needs verification against real Athena query output, not a mock.

Run with: python3 -m pytest test_lambda_function.py -v
"""
from unittest.mock import MagicMock, patch

import lambda_function as lf


@patch.object(lf, "s3_client")
@patch.object(lf, "athena_client")
def test_rebuild_mart_runs_drop_delete_ctas_in_order(mock_athena, mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    lf.ATHENA_WORKGROUP = "test-workgroup"
    lf.ATHENA_RESULTS_LOCATION = "s3://test-bucket/athena-results/"

    mock_athena.start_query_execution.side_effect = [
        {"QueryExecutionId": "drop-query-id"},
        {"QueryExecutionId": "ctas-query-id"},
    ]
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }

    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "marts/test_mart/old_file.parquet"}]}
    ]
    mock_s3.get_paginator.return_value = mock_paginator

    result = lf._rebuild_mart("test_mart", "SELECT 1")

    assert result == {"mart": "test_mart", "status": "success"}
    assert mock_athena.start_query_execution.call_count == 2
    drop_call, ctas_call = mock_athena.start_query_execution.call_args_list
    assert "DROP TABLE IF EXISTS test_mart" in drop_call.kwargs["QueryString"]
    assert "CREATE TABLE test_mart" in ctas_call.kwargs["QueryString"]
    assert "s3://test-bucket/marts/test_mart/" in ctas_call.kwargs["QueryString"]
    mock_s3.delete_objects.assert_called_once_with(
        Bucket="test-bucket",
        Delete={"Objects": [{"Key": "marts/test_mart/old_file.parquet"}]},
    )
    print("PASS: rebuild sequence is DROP -> S3 delete -> CTAS, in that order")


@patch.object(lf, "s3_client")
@patch.object(lf, "athena_client")
def test_ctas_failure_raises_with_athena_reason(mock_athena, mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    lf.ATHENA_WORKGROUP = "test-workgroup"
    lf.ATHENA_RESULTS_LOCATION = "s3://test-bucket/athena-results/"

    mock_athena.start_query_execution.side_effect = [
        {"QueryExecutionId": "drop-query-id"},
        {"QueryExecutionId": "ctas-query-id"},
    ]
    mock_athena.get_query_execution.side_effect = [
        {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        {"QueryExecution": {"Status": {
            "State": "FAILED",
            "StateChangeReason": "COLUMN_NOT_FOUND: line 3: Column 'bogus_column' cannot be resolved",
        }}},
    ]
    mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

    result = lf.lambda_handler({}, None)

    failed = [r for r in result["results"] if r["status"] == "error"][0]
    assert "bogus_column" in failed["error"]
    print("PASS: CTAS failure surfaces Athena's real error message, not a generic one")


@patch.object(lf, "s3_client")
@patch.object(lf, "athena_client")
def test_one_mart_failure_does_not_block_the_others(mock_athena, mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    lf.ATHENA_WORKGROUP = "test-workgroup"
    lf.ATHENA_RESULTS_LOCATION = "s3://test-bucket/athena-results/"

    call_count = {"n": 0}

    def start_query_side_effect(**kwargs):
        call_count["n"] += 1
        return {"QueryExecutionId": f"query-{call_count['n']}"}

    def get_query_side_effect(**kwargs):
        if kwargs["QueryExecutionId"] == "query-2":
            return {"QueryExecution": {"Status": {"State": "FAILED", "StateChangeReason": "simulated failure"}}}
        return {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}}

    mock_athena.start_query_execution.side_effect = start_query_side_effect
    mock_athena.get_query_execution.side_effect = get_query_side_effect
    mock_s3.get_paginator.return_value.paginate.return_value = [{"Contents": []}]

    result = lf.lambda_handler({}, None)

    statuses = [r["status"] for r in result["results"]]
    assert len(result["results"]) == len(lf.MART_QUERIES)
    assert "error" in statuses
    assert "success" in statuses
    print("PASS: one mart's CTAS failure doesn't block the rest from rebuilding")


@patch.object(lf, "s3_client")
def test_delete_all_objects_paginates_and_returns_count(mock_s3):
    lf.BUCKET_NAME = "test-bucket"
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "marts/x/a.parquet"}, {"Key": "marts/x/b.parquet"}]},
        {"Contents": [{"Key": "marts/x/c.parquet"}]},
        {"Contents": []},
    ]
    mock_s3.get_paginator.return_value = mock_paginator

    deleted = lf._delete_all_objects("marts/x/")

    assert deleted == 3
    assert mock_s3.delete_objects.call_count == 2
    print("PASS: delete_all_objects paginates correctly and skips empty pages")


def test_all_seven_marts_are_defined():
    expected = {
        "daily_calls_booked_by_source",
        "cost_per_booking_by_channel",
        "bookings_trend_over_time",
        "channel_attribution",
        "booking_volume_by_timeslot",
        "meeting_load_per_employee",
        "channel_performance_summary",
    }
    assert set(lf.MART_QUERIES.keys()) == expected
    print("PASS: all 7 marts from Section 9/10 of the design doc are defined")


if __name__ == "__main__":
    test_rebuild_mart_runs_drop_delete_ctas_in_order()
    test_ctas_failure_raises_with_athena_reason()
    test_one_mart_failure_does_not_block_the_others()
    test_delete_all_objects_paginates_and_returns_count()
    test_all_seven_marts_are_defined()
    print("\nAll tests passed.")
