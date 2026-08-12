"""
Local sanity tests for script.py - no real AWS calls.

Run with: python3 -m pytest test_script.py -v
"""
import json
from unittest.mock import MagicMock, patch

import pandas as pd

import script as sc


# Real sample payload shape from the requirements doc's invitee.created example.
SAMPLE_BOOKING_EVENT = {
    "created_at": "2025-07-09T06:04:34.000000Z",
    "event": "invitee.created",
    "payload": {
        "created_at": "2025-07-09T06:04:33.761871Z",
        "email": "chrisgarzon19@gmail.com",
        "name": "Ninad Magdum",
        "questions_and_answers": [
            {"answer": "+91 91303 02575", "position": 0, "question": "What is your phone number?"}
        ],
        "rescheduled": False,
        "scheduled_event": {
            "created_at": "2025-07-09T06:04:33.743275Z",
            "end_time": "2025-07-09T18:00:00.000000Z",
            "event_memberships": [
                {
                    "user": "https://api.calendly.com/users/76e8f2b2-b38c-41c8-826f-4b61f2ba22ba",
                    "user_email": "zan@dataengineeracademy.com",
                    "user_name": "Zan Strmec",
                }
            ],
            "name": "Data Engineer Academy Info Session",
            "start_time": "2025-07-09T17:45:00.000000Z",
            "status": "active",
            "uri": "https://api.calendly.com/scheduled_events/1ac9e88e-eae3-4e4b-b979-d770cff02d72",
        },
        "status": "active",
        "timezone": "Asia/Calcutta",
        "tracking": {
            "utm_campaign": "facebook_paid_ads",
            "utm_source": None,
            "utm_medium": None,
            "utm_content": None,
            "utm_term": None,
        },
        "uri": "https://api.calendly.com/scheduled_events/1ac9e88e-eae3-4e4b-b979-d770cff02d72/invitees/22a0f2d6-1bde-4fc1-95c1-d969df1da21d",
    },
}

SAMPLE_SPEND_FILE = [
    {"date": "2025-06-24", "channel": "facebook_paid_ads", "spend": 653.28},
    {"date": "2025-06-24", "channel": "youtube_paid_ads", "spend": 487.59},
    {"date": "2025-06-24", "channel": "tiktok_paid_ads", "spend": 345.12},
]


def test_flatten_real_sample_booking_event():
    """Exercises the flattener against the actual sample payload from the
    requirements doc, not a simplified stand-in - catches field-path
    mistakes a synthetic fixture would hide."""
    row = sc._flatten_booking_event(SAMPLE_BOOKING_EVENT, "raw/calendly_webhook_events/dt=2025-07-09/x.json")

    assert row["booking_id"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    assert row["invitee_name"] == "Ninad Magdum"
    assert row["invitee_email"] == "chrisgarzon19@gmail.com"
    assert row["channel"] == "facebook_paid_ads"
    assert row["meeting_name"] == "Data Engineer Academy Info Session"
    assert row["employee_email"] == "zan@dataengineeracademy.com"
    assert row["employee_name"] == "Zan Strmec"
    assert row["timezone"] == "Asia/Calcutta"
    assert row["rescheduled"] is False
    print("PASS: real sample booking payload flattens correctly")


def test_flatten_spend_records():
    rows = sc._flatten_spend_records(SAMPLE_SPEND_FILE, "raw/calendly_spend/dt=2025-06-24/spend_data_2025-06-24.json")

    assert len(rows) == 3
    assert rows[0]["channel"] == "facebook_paid_ads"
    assert rows[0]["spend"] == 653.28
    assert all(r["source_file"] == "raw/calendly_spend/dt=2025-06-24/spend_data_2025-06-24.json" for r in rows)
    print("PASS: spend records flatten correctly, each tagged with source file")


def test_booking_id_from_uri():
    uri = "https://api.calendly.com/scheduled_events/1ac9e88e-eae3-4e4b-b979-d770cff02d72/invitees/22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    assert sc._booking_id_from_uri(uri) == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    assert sc._booking_id_from_uri(None) is None
    print("PASS: booking_id correctly extracted as final URI path segment")


@patch.object(sc, "s3_client")
def test_transform_bookings_reads_all_pages_and_types_dates(mock_s3):
    """Confirms pagination is actually used (not just a single ListObjects
    call) and that timestamp columns come back as real datetimes, not
    strings - matters for anything downstream doing date math in Athena."""
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "raw/calendly_webhook_events/dt=2025-07-09/a.json"}]},
        {"Contents": [{"Key": "raw/calendly_webhook_events/dt=2025-07-10/b.json"}]},
    ]
    mock_s3.get_paginator.return_value = mock_paginator

    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: json.dumps(SAMPLE_BOOKING_EVENT).encode("utf-8"))
    }

    df = sc.transform_bookings("test-bucket")

    assert len(df) == 2  # one row per raw file, both pages read
    assert pd.api.types.is_datetime64_any_dtype(df["booked_at"])
    assert pd.api.types.is_datetime64_any_dtype(df["meeting_start_time"])
    print("PASS: transform_bookings paginates fully and types timestamp columns")


@patch.object(sc, "s3_client")
def test_transform_bookings_skips_unreadable_file_without_failing_the_run(mock_s3):
    """One malformed raw file must not abort the whole transform - matches
    the fault-isolation pattern used throughout this project's Lambdas."""
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"Contents": [
            {"Key": "raw/calendly_webhook_events/dt=2025-07-09/good.json"},
            {"Key": "raw/calendly_webhook_events/dt=2025-07-09/bad.json"},
        ]}
    ]
    mock_s3.get_paginator.return_value = mock_paginator

    def get_object_side_effect(Bucket, Key):
        if "bad" in Key:
            return {"Body": MagicMock(read=lambda: b"not valid json")}
        return {"Body": MagicMock(read=lambda: json.dumps(SAMPLE_BOOKING_EVENT).encode("utf-8"))}

    mock_s3.get_object.side_effect = get_object_side_effect

    df = sc.transform_bookings("test-bucket")

    assert len(df) == 1  # only the good file made it through
    print("PASS: one malformed raw file is skipped, not fatal to the run")


def test_get_job_param_parses_argv():
    original_argv = sc.sys.argv
    sc.sys.argv = ["script.py", "--BUCKET_NAME", "my-test-bucket"]
    try:
        assert sc._get_job_param("BUCKET_NAME") == "my-test-bucket"
        assert sc._get_job_param("MISSING_PARAM", "fallback") == "fallback"
    finally:
        sc.sys.argv = original_argv
    print("PASS: job parameters parsed correctly from --KEY value argv pairs")


if __name__ == "__main__":
    test_flatten_real_sample_booking_event()
    test_flatten_spend_records()
    test_booking_id_from_uri()
    test_transform_bookings_reads_all_pages_and_types_dates()
    test_transform_bookings_skips_unreadable_file_without_failing_the_run()
    test_get_job_param_parses_argv()
    print("\nAll tests passed.")
