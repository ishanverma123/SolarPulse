from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, dayofmonth, hour, month, to_timestamp, when, year


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
    speed_score = (
        when(col("speed") >= 700, 5)
        .when(col("speed") >= 600, 4)
        .when(col("speed") >= 500, 3)
        .when(col("speed") >= 450, 2)
        .otherwise(1)
    )
    density_score = (
        when(col("density") >= 10, 5)
        .when(col("density") >= 9, 4)
        .when(col("density") >= 8, 3)
        .when(col("density") >= 7, 2)
        .otherwise(1)
    )
    magnetic_score = (
        when(col("bt") >= 8, 5)
        .when(col("bt") >= 7, 4)
        .when(col("bt") >= 6, 3)
        .when(col("bt") >= 5, 2)
        .otherwise(1)
    )
    risk_band = (
        when(col("disturbance_score") >= 13, "extreme")
        .when(col("disturbance_score") >= 10, "high")
        .when(col("disturbance_score") >= 7, "elevated")
        .otherwise("baseline")
    )
    enriched = (
        df.withColumn("speed_score", speed_score)
        .withColumn("density_score", density_score)
        .withColumn("magnetic_score", magnetic_score)
        .withColumn(
            "disturbance_score",
            col("speed_score") + col("density_score") + col("magnetic_score"),
        )
    )
    return enriched.withColumn("risk_band", risk_band)


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
