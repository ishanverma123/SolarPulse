from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    expr,
    from_json,
    max as spark_max,
    min as spark_min,
    stddev_pop,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


EVENT_SCHEMA = StructType(
    [
        StructField("time_tag", StringType(), False),
        StructField("speed", DoubleType(), False),
        StructField("density", DoubleType(), False),
        StructField("temperature", DoubleType(), True),
        StructField("bx", DoubleType(), True),
        StructField("by", DoubleType(), True),
        StructField("bz", DoubleType(), True),
        StructField("bt", DoubleType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming job for SolarPulse.")
    parser.add_argument("--stream-name", required=True, help="Kinesis stream name.")
    parser.add_argument("--region", default="eu-west-1", help="AWS region for Kinesis.")
    parser.add_argument("--output", required=True, help="S3 or local output path.")
    parser.add_argument(
        "--checkpoint",
        required=True,
        help="Checkpoint location for Structured Streaming state.",
    )
    parser.add_argument(
        "--starting-position",
        default="LATEST",
        choices=("LATEST", "TRIM_HORIZON"),
        help="Initial Kinesis position.",
    )
    return parser.parse_args()


def build_speed_layer(spark: SparkSession, stream_name: str, region: str, starting_position: str):
    raw_stream = (
        spark.readStream.format("kinesis")
        .option("streamName", stream_name)
        .option("region", region)
        .option("initialPosition", starting_position)
        .load()
    )

    parsed = (
        raw_stream.selectExpr("CAST(data AS STRING) AS payload")
        .select(from_json(col("payload"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("event_ts", expr("to_timestamp(time_tag)"))
        .withColumn("ingested_at", current_timestamp())
        .withColumn(
            "disturbance_score",
            expr(
                """
                (CASE
                    WHEN speed >= 700 THEN 5
                    WHEN speed >= 600 THEN 4
                    WHEN speed >= 500 THEN 3
                    WHEN speed >= 450 THEN 2
                    ELSE 1
                 END)
                +
                (CASE
                    WHEN density >= 10 THEN 5
                    WHEN density >= 9 THEN 4
                    WHEN density >= 8 THEN 3
                    WHEN density >= 7 THEN 2
                    ELSE 1
                 END)
                +
                (CASE
                    WHEN bt >= 8 THEN 5
                    WHEN bt >= 7 THEN 4
                    WHEN bt >= 6 THEN 3
                    WHEN bt >= 5 THEN 2
                    ELSE 1
                 END)
                """
            ),
        )
    )

    windows = [("1 minute", "1 minute"), ("5 minutes", "1 minute"), ("15 minutes", "5 minutes")]
    aggregates = []
    for window_duration, slide_duration in windows:
        frame = (
            parsed.groupBy(window(col("event_ts"), window_duration, slide_duration))
            .agg(
                avg("speed").alias("rolling_avg_speed"),
                spark_min("speed").alias("rolling_min_speed"),
                spark_max("speed").alias("rolling_max_speed"),
                avg("density").alias("rolling_avg_density"),
                avg("disturbance_score").alias("rolling_avg_disturbance"),
                stddev_pop("speed").alias("speed_volatility"),
            )
            .withColumn("window_label", expr(f"'{window_duration}'"))
        )
        aggregates.append(frame)
    return aggregates[0].unionByName(aggregates[1]).unionByName(aggregates[2])


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("SolarPulse Speed Layer").getOrCreate()

    speed_layer = build_speed_layer(
        spark=spark,
        stream_name=args.stream_name,
        region=args.region,
        starting_position=args.starting_position,
    )

    query = (
        speed_layer.writeStream.format("parquet")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
