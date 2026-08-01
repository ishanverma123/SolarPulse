from __future__ import annotations

import json
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
output_uri = st.sidebar.text_input("Output path", value=st.secrets.get("output_uri", DEFAULT_OUTPUT_URI))


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
