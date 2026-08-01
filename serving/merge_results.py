from __future__ import annotations

import argparse
import json

import boto3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge batch and speed-layer outputs.")
    parser.add_argument("--batch-summary", required=True, help="Path to batch summary JSON.")
    parser.add_argument("--batch-percentiles", required=True, help="Path to batch percentiles JSON.")
    parser.add_argument("--live-snapshot", required=True, help="Path to live snapshot JSON.")
    return parser.parse_args()


def split_s3_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def load_json(path: str) -> dict:
    if path.startswith("s3://"):
        bucket, key = split_s3_uri(path)
        response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def main() -> None:
    args = parse_args()
    summary = load_json(args.batch_summary)
    percentiles = load_json(args.batch_percentiles)
    live = load_json(args.live_snapshot)

    merged = {
        "time_tag": live["time_tag"],
        "current_speed": live["current_speed"],
        "rolling_avg_speed": live["rolling_avg_speed"],
        "historical_avg_speed": summary["avg_speed"],
        "historical_p95_speed": percentiles["p95"],
        "historical_p99_speed": percentiles["p99"],
        "disturbance_score": live["disturbance_score"],
        "risk_band": live["risk_band"],
    }
    print(json.dumps(merged, indent=2))


if __name__ == "__main__":
    main()
