"""
Local sanity tests for script.py - no real AWS calls.

Run with: python3 -m pytest test_script.py -v
"""
import json
from unittest.mock import MagicMock, patch

import pandas as pd

import script as sc

SAMPLE_METADATA = {
    "hashedId": "8hunphufxp",
    "name": "InsightFlow Intro Video",
    "type": "Video",
    "duration": 125.4,
    "created": "2026-01-01T00:00:00Z",
    "updated": "2026-06-01T00:00:00Z",
    "status": "ready",
}

SAMPLE_ENGAGEMENT = {
    "stats": {"play_count": 452, "engagement": 0.63},
}

SAMPLE_EVENTS = [
    {"received_at": "2026-08-10T12:00:00Z", "event_key": "e2", "visitor_key": "v2"},
    {"received_at": "2026-08-10T11:00:00Z", "event_key": "e1", "visitor_key": "v1"},
]


def _mock_paginator(pages):
    paginator = MagicMock()
    paginator.paginate.return_value = pages
    return paginator


def test_extract_media_id_from_key():
    key = "raw/wistia_stats/media_id=8hunphufxp/dt=2026-08-10/metadata.json"
    assert sc._extract_media_id(key) == "8hunphufxp"
    assert sc._extract_media_id("raw/no_media_id_here.json") is None
    print("PASS: media_id correctly extracted from partitioned S3 key")


@patch.object(sc, "s3_client")
def test_transform_metadata_filters_to_metadata_files_only(mock_s3):
    """Confirms the suffix_filter actually excludes engagement.json/
    events_*.json from the same prefix listing - critical since all three
    file types live under the same media_id=/dt= partition."""
    mock_s3.get_paginator.return_value = _mock_paginator([
        {"Contents": [
            {"Key": "raw/wistia_stats/media_id=8hunphufxp/dt=2026-08-10/metadata.json"},
            {"Key": "raw/wistia_stats/media_id=8hunphufxp/dt=2026-08-10/engagement.json"},
            {"Key": "raw/wistia_stats/media_id=8hunphufxp/dt=2026-08-10/events_120000.json"},
        ]}
    ])
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps(SAMPLE_METADATA).encode("utf-8"))
    }

    df = sc.transform_metadata("test-bucket")

    assert len(df) == 1  # only the metadata.json file, not the other two
    assert df.iloc[0]["media_id"] == "8hunphufxp"
    assert df.iloc[0]["hashed_id"] == "8hunphufxp"
    assert df.iloc[0]["name"] == "InsightFlow Intro Video"
    assert pd.api.types.is_datetime64_any_dtype(df["created"])
    print("PASS: transform_metadata reads only metadata.json files, typed correctly")


@patch.object(sc, "s3_client")
def test_transform_engagement_generic_flatten(mock_s3):
    """Locks in the deliberately generic json_normalize behavior - if
    Wistia's real response shape differs from this guess, this test (not
    a production run) is where that surfaces first."""
    mock_s3.get_paginator.return_value = _mock_paginator([
        {"Contents": [{"Key": "raw/wistia_stats/media_id=8hunphufxp/dt=2026-08-10/engagement.json"}]}
    ])
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps(SAMPLE_ENGAGEMENT).encode("utf-8"))
    }

    df = sc.transform_engagement("test-bucket")

    assert len(df) == 1
    assert df.iloc[0]["media_id"] == "8hunphufxp"
    assert df.iloc[0]["stats.play_count"] == 452  # json_normalize flattens nested dicts with dot notation
    print("PASS: engagement data flattened generically via json_normalize")


@patch.object(sc, "s3_client")
def test_transform_events_one_row_per_event_across_files(mock_s3):
    """Each events_*.json file contains a LIST of events, not a single
    record - this must expand into one row per event, tagged with the
    media_id parsed from the file's own path."""
    mock_s3.get_paginator.return_value = _mock_paginator([
        {"Contents": [{"Key": "raw/wistia_stats/media_id=9k4tbcdfg0/dt=2026-08-10/events_120000.json"}]}
    ])
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps(SAMPLE_EVENTS).encode("utf-8"))
    }

    df = sc.transform_events("test-bucket")

    assert len(df) == 2  # both events in the list, not 1 row per file
    assert set(df["event_key"]) == {"e1", "e2"}
    assert all(df["media_id"] == "9k4tbcdfg0")
    assert pd.api.types.is_datetime64_any_dtype(df["received_at"])
    print("PASS: events file (a list) expands into one row per event")


@patch.object(sc, "s3_client")
def test_malformed_file_skipped_without_failing_the_run(mock_s3):
    mock_s3.get_paginator.return_value = _mock_paginator([
        {"Contents": [
            {"Key": "raw/wistia_stats/media_id=8hunphufxp/dt=2026-08-10/metadata.json"},
            {"Key": "raw/wistia_stats/media_id=bad/dt=2026-08-10/metadata.json"},
        ]}
    ])

    def get_object_side_effect(Bucket, Key):
        if "bad" in Key:
            return {"Body": MagicMock(read=lambda: b"not valid json")}
        return {"Body": MagicMock(read=lambda: json.dumps(SAMPLE_METADATA).encode("utf-8"))}

    mock_s3.get_object.side_effect = get_object_side_effect

    df = sc.transform_metadata("test-bucket")

    assert len(df) == 1  # only the good file
    print("PASS: one malformed file skipped, doesn't fail the whole run")


@patch.object(sc, "s3_client")
def test_no_files_returns_empty_dataframe_not_error(mock_s3):
    mock_s3.get_paginator.return_value = _mock_paginator([{"Contents": []}])

    df = sc.transform_events("test-bucket")

    assert df.empty
    print("PASS: no matching files -> empty DataFrame, no exception")


if __name__ == "__main__":
    test_extract_media_id_from_key()
    test_transform_metadata_filters_to_metadata_files_only()
    test_transform_engagement_generic_flatten()
    test_transform_events_one_row_per_event_across_files()
    test_malformed_file_skipped_without_failing_the_run()
    test_no_files_returns_empty_dataframe_not_error()
    print("\nAll tests passed.")
