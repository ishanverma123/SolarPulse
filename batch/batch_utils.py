from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import avg, col, dayofmonth, hour, month, to_timestamp, year

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from speed.anomaly import classify, disturbance_score


REQUIRED_COLUMNS = (
    "time_tag",
    "speed",
    "density",
    "temperature",
    "bx",
    "by",
    "bz",
    "bt",
)


def validate_columns(df: DataFrame) -> None:
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(f"Input dataset is missing required columns: {', '.join(missing)}")


def clean_dataset(df: DataFrame) -> DataFrame:
    return (
        df.dropna(subset=["time_tag", "speed", "density", "bt"])
        .filter(col("speed") > 0)
        .filter(col("density") > 0)
        .filter(col("bt") >= 0)
    )


def add_time_columns(df: DataFrame) -> DataFrame:
    df = df.withColumn("timestamp", to_timestamp("time_tag"))
    return (
        df.withColumn("year", year("timestamp"))
        .withColumn("month", month("timestamp"))
        .withColumn("day", dayofmonth("timestamp"))
        .withColumn("hour", hour("timestamp"))
    )


def add_disturbance_score(df: DataFrame) -> DataFrame:
    disturbance_udf = F.udf(
        lambda speed, density, bt: disturbance_score(float(speed), float(density), float(bt)),
        "int",
    )
    risk_band_udf = F.udf(lambda score: classify(int(score)), "string")

    enriched = df.withColumn(
        "disturbance_score",
        disturbance_udf(col("speed"), col("density"), col("bt")),
    )
    return enriched.withColumn("risk_band", risk_band_udf(col("disturbance_score")))


def build_summary(df: DataFrame) -> dict:
    row = (
        df.agg(
            avg("speed").alias("avg_speed"),
            avg("density").alias("avg_density"),
            avg("bt").alias("avg_bt"),
            avg("disturbance_score").alias("avg_disturbance_score"),
        )
        .collect()[0]
    )
    return {
        "record_count": df.count(),
        "avg_speed": round(row["avg_speed"], 3),
        "avg_density": round(row["avg_density"], 3),
        "avg_bt": round(row["avg_bt"], 3),
        "avg_disturbance_score": round(row["avg_disturbance_score"], 3),
        "max_speed": df.agg({"speed": "max"}).collect()[0][0],
        "min_speed": df.agg({"speed": "min"}).collect()[0][0],
    }
