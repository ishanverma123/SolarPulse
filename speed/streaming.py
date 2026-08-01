from __future__ import annotations

import argparse
import csv
import json
from collections import deque
from statistics import mean

from anomaly import classify, disturbance_score


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local rolling-window speed-layer scaffold.")
    parser.add_argument("--input", required=True, help="Historical CSV used as a replay source.")
    parser.add_argument("--window-size", type=int, default=5, help="Rolling window size in records.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    window = deque(maxlen=args.window_size)

    with open(args.input, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
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
            snapshot = {
                "time_tag": event["time_tag"],
                "current_speed": event["speed"],
                "rolling_avg_speed": round(mean(speeds), 3),
                "rolling_min_speed": min(speeds),
                "rolling_max_speed": max(speeds),
                "disturbance_score": event["disturbance_score"],
                "risk_band": event["risk_band"],
            }
            print(json.dumps(snapshot))


if __name__ == "__main__":
    main()
