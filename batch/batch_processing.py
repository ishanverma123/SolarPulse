from __future__ import annotations

import argparse
import json
from pathlib import Path

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count

from batch_utils import add_disturbance_score, add_time_columns, build_summary, clean_dataset, validate_columns


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the SolarPulse Spark batch layer.")
    parser.add_argument("--input", required=True, help="CSV input path. Local path or S3 URI.")
    parser.add_argument("--output", required=True, help="Output directory. Local path or S3 URI.")
    parser.add_argument("--app-name", default="SolarPulse Batch Layer", help="Spark application name.")
    return parser.parse_args()


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .getOrCreate()
    )


def spark_uri(uri: str) -> str:
    if uri.startswith("s3://"):
        return uri.replace("s3://", "s3a://", 1)
    return uri


def split_s3_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key.rstrip("/")


def write_json(base_output: str, relative_name: str, payload: dict) -> None:
    if base_output.startswith("s3://"):
        bucket, prefix = split_s3_uri(base_output)
        key = f"{prefix}/{relative_name}" if prefix else relative_name
        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        return
    output_path = Path(base_output)
    output_path.mkdir(parents=True, exist_ok=True)
    with (output_path / relative_name).open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def main() -> None:
    args = parse_args()
    spark = create_spark_session(args.app_name)
    input_uri = spark_uri(args.input)
    output_uri = spark_uri(args.output)

    try:
        df = spark.read.csv(input_uri, header=True, inferSchema=True)
        validate_columns(df)

        df = clean_dataset(df)
        df = add_time_columns(df)
        df = add_disturbance_score(df).cache()

        daily_speed = (
            df.groupBy("year", "month", "day")
            .agg(
                avg("speed").alias("daily_avg_speed"),
                avg("density").alias("daily_avg_density"),
                avg("disturbance_score").alias("daily_avg_disturbance"),
            )
            .orderBy("year", "month", "day")
        )
        monthly_speed = (
            df.groupBy("year", "month")
            .agg(
                avg("speed").alias("monthly_avg_speed"),
                avg("density").alias("monthly_avg_density"),
                avg("disturbance_score").alias("monthly_avg_disturbance"),
            )
            .orderBy("year", "month")
        )
        hourly_speed = (
            df.groupBy("hour")
            .agg(
                avg("speed").alias("hourly_avg_speed"),
                avg("density").alias("hourly_avg_density"),
                avg("disturbance_score").alias("hourly_avg_disturbance"),
            )
            .orderBy("hour")
        )
        storm_events = (
            df.filter(col("speed") >= 700)
            .select("timestamp", "speed", "density", "bt", "disturbance_score", "risk_band")
            .orderBy("timestamp")
        )
        disturbance_breakdown = (
            df.groupBy("risk_band")
            .agg(
                count("*").alias("event_count"),
                avg("speed").alias("avg_speed"),
                avg("density").alias("avg_density"),
            )
            .orderBy("risk_band")
        )

        correlations = {
            "speed_density": df.stat.corr("speed", "density"),
            "speed_bt": df.stat.corr("speed", "bt"),
            "density_bt": df.stat.corr("density", "bt"),
        }
        quantiles = df.approxQuantile("speed", [0.25, 0.50, 0.75, 0.90, 0.95, 0.99], 0.01)
        percentiles = {
            "p25": quantiles[0],
            "p50": quantiles[1],
            "p75": quantiles[2],
            "p90": quantiles[3],
            "p95": quantiles[4],
            "p99": quantiles[5],
        }
        summary = build_summary(df)

        daily_speed.write.mode("overwrite").parquet(f"{output_uri}/daily_speed")
        monthly_speed.write.mode("overwrite").parquet(f"{output_uri}/monthly_speed")
        hourly_speed.write.mode("overwrite").parquet(f"{output_uri}/hourly_speed")
        storm_events.write.mode("overwrite").parquet(f"{output_uri}/storm_events")
        disturbance_breakdown.write.mode("overwrite").parquet(f"{output_uri}/disturbance_breakdown")

        write_json(args.output, "summary.json", summary)
        write_json(args.output, "percentiles.json", percentiles)
        write_json(args.output, "correlations.json", correlations)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
