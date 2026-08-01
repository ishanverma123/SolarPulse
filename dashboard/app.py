from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import boto3
import pyarrow.parquet as pq


st.set_page_config(page_title="SolarPulse", layout="wide")
st.title("SolarPulse Dashboard")

DEFAULT_OUTPUT_URI = str(Path(__file__).resolve().parents[1] / "output")


def get_output_uri() -> str:
    env_uri = os.getenv("SOLARPULSE_OUTPUT_URI")
    if env_uri:
        return env_uri

    try:
        return st.secrets.get("output_uri", DEFAULT_OUTPUT_URI)
    except Exception:
        return DEFAULT_OUTPUT_URI


output_uri = st.sidebar.text_input("Output path", value=get_output_uri())


def is_s3_uri(value: str) -> bool:
    return value.startswith("s3://")


def split_s3_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key.rstrip("/")


def load_json(uri: str, relative_name: str) -> dict:
    if is_s3_uri(uri):
        bucket, prefix = split_s3_uri(uri)
        key = f"{prefix}/{relative_name}" if prefix else relative_name
        response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    with open(Path(uri) / relative_name, encoding="utf-8") as handle:
        return json.load(handle)


def load_daily_df(uri: str) -> pd.DataFrame | None:
    if is_s3_uri(uri):
        bucket, prefix = split_s3_uri(uri)
        base_prefix = f"{prefix}/daily_speed/" if prefix else "daily_speed/"
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=base_prefix)
        keys = [
            item["Key"]
            for item in response.get("Contents", [])
            if item["Key"].endswith(".parquet")
        ]
        if not keys:
            return None

        tables = []
        for key in keys:
            with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
                s3.download_file(bucket, key, temp_file.name)
                tables.append(pq.read_table(temp_file.name))
        if not tables:
            return None
        return pd.concat([table.to_pandas() for table in tables], ignore_index=True)

    target = Path(uri) / "daily_speed"
    try:
        return pd.read_parquet(target)
    except FileNotFoundError:
        return None


def load_latest_speed_snapshots(uri: str) -> pd.DataFrame | None:
    if not is_s3_uri(uri):
        return None

    bucket, prefix = split_s3_uri(uri)
    speed_prefix = prefix.replace("/batch", "/speed/live") if "/batch" in prefix else "speed/live"
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=f"{speed_prefix}/")
    keys = sorted(
        [
            item["Key"]
            for item in response.get("Contents", [])
            if item["Key"].endswith(".json")
        ]
    )
    if not keys:
        return None

    recent = keys[-50:]
    rows = []
    for key in recent:
        obj = s3.get_object(Bucket=bucket, Key=key)
        rows.append(json.loads(obj["Body"].read().decode("utf-8")))
    return pd.DataFrame(rows)


if is_s3_uri(output_uri):
    summary_exists = True
else:
    summary_exists = (Path(output_uri) / "summary.json").exists()

if not summary_exists:
    st.info("Run the batch layer first so the dashboard has data to display.")
    st.stop()

summary = load_json(output_uri, "summary.json")
percentiles = load_json(output_uri, "percentiles.json")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Records", summary["record_count"])
col2.metric("Avg Speed", summary["avg_speed"])
col3.metric("Max Speed", summary["max_speed"])
col4.metric("Avg Disturbance", summary["avg_disturbance_score"])

st.subheader("Historical Speed Percentiles")
st.json(percentiles)

daily_df = load_daily_df(output_uri)
if daily_df is not None and not daily_df.empty:
    daily_df["date"] = pd.to_datetime(daily_df[["year", "month", "day"]])
    fig = px.line(daily_df, x="date", y="daily_avg_speed", title="Daily Average Speed")
    st.plotly_chart(fig, use_container_width=True)

speed_df = load_latest_speed_snapshots(output_uri)
if speed_df is not None and not speed_df.empty:
    st.subheader("Recent Speed Layer Snapshots")
    latest = speed_df.iloc[-1]
    col1, col2, col3 = st.columns(3)
    col1.metric("Current Speed", latest["current_speed"])
    col2.metric("Rolling Avg Speed", latest["rolling_avg_speed"])
    col3.metric("Risk Band", latest["risk_band"])

    speed_df["time_tag"] = pd.to_datetime(speed_df["time_tag"])
    live_fig = px.line(
        speed_df,
        x="time_tag",
        y=["current_speed", "rolling_avg_speed"],
        title="Live Speed vs Rolling Average",
    )
    st.plotly_chart(live_fig, use_container_width=True)
