from __future__ import annotations

import argparse
import csv
import json
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from statistics import mean
from typing import Any

import boto3


DEFAULT_OUTPUT_SCHEMA = {
    "time_tag": None,
    "speed": 0.0,
    "density": 0.0,
    "temperature": 0.0,
    "bx": 0.0,
    "by": 0.0,
    "bz": 0.0,
    "bt": 0.0,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay SolarPulse records from CSV or a live public API.")
    parser.add_argument(
        "--source",
        choices=("csv", "api"),
        default="csv",
        help="Choose between historical CSV replay or a live public JSON API feed.",
    )
    parser.add_argument("--input", help="Historical CSV file to replay when --source csv is used.")
    parser.add_argument("--rate", type=float, default=1.0, help="Events per second when replaying a CSV file.")
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
        help="Field used as the Kinesis partition key.",
    )
    parser.add_argument(
        "--api-url",
        help="Public JSON API endpoint for the live stream source.",
    )
    parser.add_argument(
        "--api-max-polls",
        type=int,
        default=1,
        help="Maximum number of API polls to execute before stopping. Use 0 for an endless loop.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="Seconds to wait between API polls.",
    )
    return parser.parse_args()


def coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_row(raw: dict[str, Any]) -> dict[str, Any]:
    timestamp = raw.get("time_tag") or raw.get("timestamp") or raw.get("time")
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()

    row = dict(DEFAULT_OUTPUT_SCHEMA)
    row["time_tag"] = str(timestamp)
    row["speed"] = coerce_float(raw.get("speed"))
    row["density"] = coerce_float(raw.get("density"))
    row["temperature"] = coerce_float(raw.get("temperature"))
    row["bx"] = coerce_float(raw.get("bx"))
    row["by"] = coerce_float(raw.get("by"))
    row["bz"] = coerce_float(raw.get("bz"))
    row["bt"] = coerce_float(raw.get("bt"))
    return row


def emit_row(row: dict[str, Any], client: Any | None, args: argparse.Namespace) -> None:
    payload = json.dumps(row)
    if args.mode == "stdout":
        sys.stdout.write(payload + "\n")
        sys.stdout.flush()
        return

    if client is None:
        raise ValueError("A Kinesis client is required when --mode kinesis is used.")

    partition_key = row.get(args.partition_key_field, row["time_tag"])
    client.put_record(
        StreamName=args.stream_name,
        Data=payload.encode("utf-8"),
        PartitionKey=str(partition_key),
    )


def iter_csv_rows(input_path: str):
    with open(input_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield normalize_row(row)


def extract_api_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("coordinates"), list):
        coordinates = payload["coordinates"]
        intensities = [
            coerce_float(item[2] if len(item) >= 3 else item[0])
            for item in coordinates
            if isinstance(item, (list, tuple))
        ]
        if not intensities:
            intensities = [0.0]

        return [
            normalize_row(
                {
                    "time_tag": payload.get("Observation Time") or payload.get("Forecast Time") or datetime.now(timezone.utc).isoformat(),
                    "speed": mean(intensities),
                    "density": len([item for item in intensities if item > 0]),
                    "temperature": max(intensities),
                    "bx": sum(intensities) / len(intensities),
                    "by": len(coordinates),
                    "bz": 0.0,
                    "bt": max(intensities),
                }
            )
        ]

    events: list[Any]
    if isinstance(payload, dict):
        if isinstance(payload.get("features"), list):
            events = payload["features"]
        elif isinstance(payload.get("results"), list):
            events = payload["results"]
        else:
            events = [payload]
    elif isinstance(payload, list):
        events = payload
    else:
        raise ValueError("The API response must be a JSON object or array.")

    rows: list[dict[str, Any]] = []
    for event in events:
        if isinstance(event, dict) and isinstance(event.get("properties"), dict):
            props = event["properties"]
            geometry = event.get("geometry") or {}
            coordinates = geometry.get("coordinates") or []
            row = normalize_row(
                {
                    "time_tag": props.get("time") or event.get("timestamp") or event.get("time"),
                    "speed": props.get("mag") or props.get("magnitude") or props.get("speed"),
                    "density": coordinates[2] if len(coordinates) >= 3 else props.get("depth"),
                    "temperature": props.get("temperature"),
                    "bx": props.get("bx"),
                    "by": props.get("by"),
                    "bz": props.get("bz"),
                    "bt": props.get("sig") or props.get("score") or props.get("bt"),
                }
            )
            rows.append(row)
        else:
            rows.append(normalize_row(event))
    return rows


def poll_api(api_url: str) -> list[dict[str, Any]]:
    request = urllib.request.Request(
        api_url,
        headers={"User-Agent": "SolarPulse-Live-Producer/1.0"},
    )
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with urllib.request.urlopen(request, timeout=30, context=context) as response:
        body = response.read().decode("utf-8")
    payload = json.loads(body)
    return extract_api_rows(payload)


def run_csv_mode(args: argparse.Namespace, client: Any | None) -> None:
    if not args.input:
        raise ValueError("--input is required when --source csv is used.")

    delay = 1.0 / args.rate if args.rate > 0 else 0.0
    for row in iter_csv_rows(args.input):
        emit_row(row, client, args)
        if delay:
            time.sleep(delay)


def run_api_mode(args: argparse.Namespace, client: Any | None) -> None:
    if not args.api_url:
        raise ValueError("--api-url is required when --source api is used.")

    polls = 0
    while True:
        try:
            rows = poll_api(args.api_url)
        except urllib.error.URLError as error:
            print(json.dumps({"error": str(error)}), file=sys.stderr)
            time.sleep(args.poll_interval)
            continue

        for row in rows:
            emit_row(row, client, args)

        polls += 1
        if args.api_max_polls > 0 and polls >= args.api_max_polls:
            break
        if args.poll_interval > 0:
            time.sleep(args.poll_interval)


def main() -> None:
    args = parse_args()
    client = None

    if args.mode == "kinesis":
        if not args.stream_name:
            raise ValueError("--stream-name is required when --mode kinesis is used.")
        client = boto3.client("kinesis", region_name=args.region)

    if args.source == "csv":
        run_csv_mode(args, client)
    else:
        run_api_mode(args, client)


if __name__ == "__main__":
    main()
