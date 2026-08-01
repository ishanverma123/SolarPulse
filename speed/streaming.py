from __future__ import annotations

import argparse
import csv
import json
import time
from collections import deque
from statistics import mean

import boto3

from anomaly import classify, disturbance_score


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rolling-window speed layer for local CSV or Kinesis.")
    parser.add_argument("--input", help="Historical CSV used as a replay source.")
    parser.add_argument("--window-size", type=int, default=5, help="Rolling window size in records.")
    parser.add_argument(
        "--mode",
        choices=("csv", "kinesis"),
        default="csv",
        help="Consume local CSV or Amazon Kinesis.",
    )
    parser.add_argument("--stream-name", help="Kinesis stream name when --mode kinesis is used.")
    parser.add_argument("--region", default="us-east-1", help="AWS region for Kinesis.")
    parser.add_argument(
        "--iterator-type",
        default="LATEST",
        choices=("LATEST", "TRIM_HORIZON"),
        help="Initial shard iterator type for Kinesis mode.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=2.0,
        help="Seconds to wait between empty Kinesis polls.",
    )
    parser.add_argument(
        "--output-s3-uri",
        help="Optional S3 prefix like s3://bucket/speed/live to store JSON snapshots.",
    )
    return parser.parse_args()


def parse_s3_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key.rstrip("/")


def build_snapshot(row: dict[str, str | float], window: deque[dict]) -> dict:
    event = {
        "time_tag": row["time_tag"],
        "speed": float(row["speed"]),
        "density": float(row["density"]),
        "bt": float(row["bt"]),
    }
    event["disturbance_score"] = disturbance_score(
        event["speed"], event["density"], event["bt"]
    )
    event["risk_band"] = classify(event["disturbance_score"])
    window.append(event)

    speeds = [item["speed"] for item in window]
    return {
        "time_tag": event["time_tag"],
        "current_speed": event["speed"],
        "rolling_avg_speed": round(mean(speeds), 3),
        "rolling_min_speed": min(speeds),
        "rolling_max_speed": max(speeds),
        "disturbance_score": event["disturbance_score"],
        "risk_band": event["risk_band"],
        "window_size": len(window),
    }


def emit_snapshot(snapshot: dict, output_s3_uri: str | None, s3_client=None) -> None:
    print(json.dumps(snapshot))
    if output_s3_uri:
        bucket, prefix = parse_s3_uri(output_s3_uri)
        key = f"{prefix}/{snapshot['time_tag'].replace(':', '-').replace('/', '-')}.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(snapshot, indent=2).encode("utf-8"),
            ContentType="application/json",
        )


def run_csv_mode(args: argparse.Namespace, window: deque[dict]) -> None:
    if not args.input:
        raise ValueError("--input is required when --mode csv is used.")

    s3_client = boto3.client("s3", region_name=args.region) if args.output_s3_uri else None
    with open(args.input, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            snapshot = build_snapshot(row, window)
            emit_snapshot(snapshot, args.output_s3_uri, s3_client)


def run_kinesis_mode(args: argparse.Namespace, window: deque[dict]) -> None:
    if not args.stream_name:
        raise ValueError("--stream-name is required when --mode kinesis is used.")

    kinesis = boto3.client("kinesis", region_name=args.region)
    s3_client = boto3.client("s3", region_name=args.region) if args.output_s3_uri else None

    shards = kinesis.list_shards(StreamName=args.stream_name)["Shards"]
    if not shards:
        raise RuntimeError(f"No shards found in stream {args.stream_name}.")

    shard_id = shards[0]["ShardId"]
    iterator = kinesis.get_shard_iterator(
        StreamName=args.stream_name,
        ShardId=shard_id,
        ShardIteratorType=args.iterator_type,
    )["ShardIterator"]

    while True:
        response = kinesis.get_records(ShardIterator=iterator, Limit=100)
        iterator = response["NextShardIterator"]
        records = response["Records"]

        if not records:
            time.sleep(args.poll_interval)
            continue

        for record in records:
            row = json.loads(record["Data"].decode("utf-8"))
            snapshot = build_snapshot(row, window)
            emit_snapshot(snapshot, args.output_s3_uri, s3_client)


def main() -> None:
    args = parse_args()
    window = deque(maxlen=args.window_size)

    if args.mode == "csv":
        run_csv_mode(args, window)
    else:
        run_kinesis_mode(args, window)


if __name__ == "__main__":
    main()
