from __future__ import annotations

import argparse
import csv
import statistics
import time


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sequential benchmark for SolarPulse batch stats.")
    parser.add_argument("--input", required=True, help="Historical CSV path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start = time.perf_counter()
    speeds = []

    with open(args.input, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            speeds.append(float(row["speed"]))

    elapsed = time.perf_counter() - start
    print(
        {
            "records": len(speeds),
            "avg_speed": round(statistics.mean(speeds), 3),
            "max_speed": max(speeds),
            "elapsed_seconds": round(elapsed, 6),
        }
    )


if __name__ == "__main__":
    main()
