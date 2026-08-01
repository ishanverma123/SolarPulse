from __future__ import annotations

import argparse
import time

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark benchmark for SolarPulse batch stats.")
    parser.add_argument("--input", required=True, help="Historical CSV path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("SolarPulse Benchmark").getOrCreate()

    try:
        start = time.perf_counter()
        df = spark.read.csv(args.input, header=True, inferSchema=True)
        result = df.selectExpr(
            "count(*) as records",
            "avg(speed) as avg_speed",
            "max(speed) as max_speed",
        ).collect()[0]
        elapsed = time.perf_counter() - start
        print(
            {
                "records": result["records"],
                "avg_speed": round(result["avg_speed"], 3),
                "max_speed": result["max_speed"],
                "elapsed_seconds": round(elapsed, 6),
            }
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
