from __future__ import annotations

import argparse
import csv
import json
import sys
import time

import boto3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay historical SolarPulse records.")
    parser.add_argument("--input", required=True, help="Historical CSV file to replay.")
    parser.add_argument("--rate", type=float, default=1.0, help="Events per second.")
    parser.add_argument(
        "--mode",
        choices=("stdout", "kinesis"),
        default="stdout",
        help="Write events to stdout or publish them to Amazon Kinesis Data Streams.",
    )
    parser.add_argument("--stream-name", help="Kinesis stream name when --mode kinesis is used.")
    parser.add_argument("--region", default="eu-west-1", help="AWS region for Kinesis publishing.")
    parser.add_argument(
        "--partition-key-field",
        default="time_tag",
        help="CSV field used as the Kinesis partition key.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    delay = 1.0 / args.rate if args.rate > 0 else 0.0
    client = None

    if args.mode == "kinesis":
        if not args.stream_name:
            raise ValueError("--stream-name is required when --mode kinesis is used.")
        client = boto3.client("kinesis", region_name=args.region)

    with open(args.input, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            payload = json.dumps(row)
            if args.mode == "stdout":
                sys.stdout.write(payload + "\n")
                sys.stdout.flush()
            else:
                partition_key = row.get(args.partition_key_field, row["time_tag"])
                client.put_record(
                    StreamName=args.stream_name,
                    Data=payload.encode("utf-8"),
                    PartitionKey=str(partition_key),
                )
            if delay:
                time.sleep(delay)


if __name__ == "__main__":
    main()
