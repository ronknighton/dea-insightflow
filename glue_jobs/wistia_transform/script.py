"""
InsightFlow - Wistia Glue Python Shell transform job.

Reads everything under raw/wistia_stats/media_id={id}/dt={date}/, written
by the wistia_handler Lambda (metadata.json, engagement.json, and zero or
more events_{HHMMSS}.json files per pull), and writes three separate clean
Parquet tables to processed/ - NOT one combined "wistia_engagement" table
as originally sketched in Section 5 of the design doc's folder layout.

WHY THREE TABLES, NOT ONE: metadata (one row per media), engagement (one
row per media per pull - an aggregate snapshot), and events (one row per
individual visitor action) are three genuinely different grains. Cramming
them into a single table means either massive sparse columns or losing
the events' row-per-visitor-action structure entirely. Same reasoning that
led to splitting Calendly into calendly_bookings/ and calendly_spend/
rather than one combined table - see glue_jobs/calendly_transform/script.py.
Athena/QuickSight can still join across all three by media_id when needed.

FULL-REBUILD PATTERN, matching calendly_transform: every run reads
everything currently under the raw prefix and completely overwrites all
three processed/ tables. Simpler than incremental merge; cheap at this
data volume.

ASSUMPTION FLAGGED: the exact shape of Wistia's engagement.json response
was not confirmed against a live account at the time this was written
(same gap noted in wistia_handler's own docstring). transform_engagement()
below flattens whatever comes back generically (via pandas.json_normalize)
rather than assuming specific nested fields, so it degrades gracefully
against an unexpected shape instead of crashing - but the resulting
columns should be spot-checked against real output before relying on them
in a marts-layer query.

CONFIRMED AGAINST LIVE DATA (Aug 14, 2026): real Wistia events include a
"conversion_data" field that comes back as an empty dict ({}) on events
with no conversion tracked - pyarrow cannot write an empty struct to
Parquet at all (ArrowNotImplementedError, not a graceful skip). See
_stringify_nested_columns() - any dict/list-valued column is JSON-
stringified before writing, rather than special-casing this one field,
since Wistia's full event schema isn't documented anywhere available to
this project.

Job parameters (passed as --KEY value by the Glue job definition):
  --BUCKET_NAME  InsightFlow data bucket
"""

import json
import logging
import re
import sys

import awswrangler as wr
import boto3
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")

MEDIA_ID_PATTERN = re.compile(r"media_id=([^/]+)/")


def _get_job_param(name: str, default: str = None) -> str:
    """See glue_jobs/calendly_transform/script.py for why this avoids
    awsglue.utils.getResolvedOptions - identical rationale here."""
    flag = f"--{name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return default


def _list_raw_keys(bucket: str, prefix: str, suffix_filter: str = None) -> list:
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            if suffix_filter and not key.split("/")[-1].startswith(suffix_filter):
                continue
            keys.append(key)
    return keys


def _read_json(bucket: str, key: str):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def _extract_media_id(key: str) -> str:
    match = MEDIA_ID_PATTERN.search(key)
    return match.group(1) if match else None


def transform_metadata(bucket: str) -> pd.DataFrame:
    keys = _list_raw_keys(bucket, "raw/wistia_stats/", suffix_filter="metadata")
    logger.info("Found %d raw metadata files", len(keys))

    rows = []
    for key in keys:
        try:
            data = _read_json(bucket, key)
            row = {
                "media_id": _extract_media_id(key),
                "hashed_id": data.get("hashedId") or data.get("hashed_id"),
                "name": data.get("name"),
                "type": data.get("type"),
                "duration": data.get("duration"),
                "created": data.get("created"),
                "updated": data.get("updated"),
                "status": data.get("status"),
                "source_file": key,
            }
            rows.append(row)
        except Exception:
            logger.exception("Skipping unreadable metadata file: %s", key)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["created"] = pd.to_datetime(df["created"], errors="coerce", utc=True)
        df["updated"] = pd.to_datetime(df["updated"], errors="coerce", utc=True)
    return df


def transform_engagement(bucket: str) -> pd.DataFrame:
    """Generic flatten - see ASSUMPTION FLAGGED in module docstring."""
    keys = _list_raw_keys(bucket, "raw/wistia_stats/", suffix_filter="engagement")
    logger.info("Found %d raw engagement files", len(keys))

    frames = []
    for key in keys:
        try:
            data = _read_json(bucket, key)
            flat = pd.json_normalize(data)
            flat["media_id"] = _extract_media_id(key)
            flat["source_file"] = key
            frames.append(flat)
        except Exception:
            logger.exception("Skipping unreadable engagement file: %s", key)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def transform_events(bucket: str) -> pd.DataFrame:
    keys = _list_raw_keys(bucket, "raw/wistia_stats/", suffix_filter="events_")
    logger.info("Found %d raw events files", len(keys))

    rows = []
    for key in keys:
        try:
            events = _read_json(bucket, key)
            media_id = _extract_media_id(key)
            for event in events:
                event_row = dict(event)
                event_row["media_id"] = media_id
                event_row["source_file"] = key
                rows.append(event_row)
        except Exception:
            logger.exception("Skipping unreadable events file: %s", key)

    df = pd.DataFrame(rows)
    if not df.empty and "received_at" in df.columns:
        df["received_at"] = pd.to_datetime(df["received_at"], errors="coerce", utc=True)
    return df


def _stringify_nested_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pyarrow cannot write a struct-typed column that has zero child fields -
    a real failure hit against live Wistia event data, not a hypothetical:
    events with no conversion tracked come back with "conversion_data": {}
    (an empty dict), and pyarrow has no field to infer a schema from,
    raising ArrowNotImplementedError. Rather than special-case that one
    field (there could be others - Wistia's schema isn't fully documented
    anywhere this project has access to), any column holding dict or list
    values gets JSON-stringified before writing, sidestepping pyarrow's
    struct inference entirely. A record with genuinely populated nested
    data (e.g. a real conversion click) also becomes a JSON string -
    still fully inspectable in Athena via json_extract_scalar if ever
    needed, just not natively typed as a nested column.
    """
    df = df.copy()
    for col in df.columns:
        has_nested = df[col].apply(lambda v: isinstance(v, (dict, list))).any()
        if has_nested:
            df[col] = df[col].apply(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)
    return df


def _write_if_not_empty(df: pd.DataFrame, bucket: str, table_name: str) -> None:
    if df.empty:
        logger.info("No data for %s - skipping write (leaving prior processed/ output untouched)", table_name)
        return
    df = _stringify_nested_columns(df)
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/processed/{table_name}/",
        dataset=True,
        mode="overwrite",
    )
    logger.info("Wrote %d rows to processed/%s/", len(df), table_name)


def main():
    bucket = _get_job_param("BUCKET_NAME")
    if not bucket:
        raise ValueError("--BUCKET_NAME job parameter is required")

    _write_if_not_empty(transform_metadata(bucket), bucket, "wistia_media_metadata")
    _write_if_not_empty(transform_engagement(bucket), bucket, "wistia_engagement")
    _write_if_not_empty(transform_events(bucket), bucket, "wistia_events")


if __name__ == "__main__":
    main()