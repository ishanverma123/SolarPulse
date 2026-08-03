from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import plotly.express as px
import pyarrow.parquet as pq
import streamlit as st


st.set_page_config(page_title="SolarPulse", layout="wide")

st.markdown(
    """
    <style>
    .stApp {
        background: linear-gradient(180deg, #07111f 0%, #0b1c33 52%, #0c1826 100%);
        color: #f7fbff;
    }
    .dashboard-hero {
        background: linear-gradient(135deg, rgba(13, 74, 131, 0.85), rgba(5, 153, 184, 0.85));
        padding: 1.25rem 1.5rem;
        border-radius: 18px;
        box-shadow: 0 8px 30px rgba(0, 0, 0, 0.25);
        margin-bottom: 1rem;
    }
    .dashboard-hero h1 {
        margin: 0 0 0.25rem 0;
        color: #ffffff;
    }
    .dashboard-hero p {
        margin: 0;
        color: #d9f8ff;
        font-size: 0.98rem;
    }
    .data-pill {
        display: inline-block;
        background: rgba(255,255,255,0.11);
        border: 1px solid rgba(255,255,255,0.18);
        color: #eaf8ff;
        border-radius: 999px;
        padding: 0.35rem 0.75rem;
        margin-right: 0.5rem;
        margin-top: 0.5rem;
        font-size: 0.82rem;
    }
    div[data-testid="stMetricLabel"] {
        color: #b8d7ef !important;
    }
    div[data-testid="stMetricValue"] {
        color: #ffffff !important;
        font-weight: 700;
    }
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="dashboard-hero">
        <h1>SolarPulse Dashboard</h1>
        <p>Real-time batch intelligence with an AWS-ready Lambda Architecture control surface.</p>
        <div class="data-pill">Batch → Historical analytics</div>
        <div class="data-pill">Speed → Live snapshots</div>
        <div class="data-pill">Serving → Streamlit control plane</div>
    </div>
    """,
    unsafe_allow_html=True,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_URI = str(PROJECT_ROOT / "output")
DEFAULT_INPUT_URI = str(PROJECT_ROOT / "data" / "historical_space_weather.csv")
BATCH_URI_SUFFIX = "/batch"
LIVE_SPEED_URI_SUFFIX = "/speed/live"
SUMMARY_JSON_NAME = "summary.json"


def get_output_uri() -> str:
    env_uri = os.getenv("SOLARPULSE_OUTPUT_URI")
    if env_uri:
        return env_uri

    try:
        return st.secrets.get("output_uri", DEFAULT_OUTPUT_URI)
    except Exception:
        return DEFAULT_OUTPUT_URI


def get_input_uri() -> str:
    env_uri = os.getenv("SOLARPULSE_INPUT_URI")
    if env_uri:
        return env_uri

    try:
        return st.secrets.get("input_uri", DEFAULT_INPUT_URI)
    except Exception:
        return DEFAULT_INPUT_URI


def is_s3_uri(value: str) -> bool:
    return value.startswith("s3://")


def split_s3_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key.rstrip("/")


def run_command(command: list[str]) -> tuple[bool, str]:
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    combined = "\n".join(part for part in [completed.stdout, completed.stderr] if part)
    return completed.returncode == 0, combined.strip()


def check_java_runtime() -> tuple[bool, str]:
    java_home = os.getenv("JAVA_HOME")
    candidates: list[str] = []

    if java_home:
        java_binary = Path(java_home) / "bin" / "java"
        if java_binary.exists():
            candidates.append(str(java_binary))

    candidates.append("java")

    for candidate in candidates:
        completed = subprocess.run(
            [candidate, "-version"],
            capture_output=True,
            text=True,
            check=False,
            env=os.environ.copy(),
        )
        combined = "\n".join(part for part in [completed.stdout, completed.stderr] if part)
        if completed.returncode == 0:
            return True, combined.strip()

    return False, "No usable Java runtime found on JAVA_HOME or PATH."


def compute_speed_output_uri(base_output_uri: str) -> str | None:
    if not is_s3_uri(base_output_uri):
        return None

    bucket, prefix = split_s3_uri(base_output_uri)
    if prefix.endswith(BATCH_URI_SUFFIX):
        speed_prefix = prefix.removesuffix(BATCH_URI_SUFFIX) + LIVE_SPEED_URI_SUFFIX
    elif prefix:
        speed_prefix = f"{prefix}{LIVE_SPEED_URI_SUFFIX}"
    else:
        speed_prefix = LIVE_SPEED_URI_SUFFIX.lstrip("/")
    return f"s3://{bucket}/{speed_prefix}"


def safe_summary_exists(output_uri: str) -> bool:
    if is_s3_uri(output_uri):
        bucket, prefix = split_s3_uri(output_uri)
        s3 = boto3.client("s3")
        try:
            response = s3.list_objects_v2(
                Bucket=bucket,
                Prefix=f"{prefix}/{SUMMARY_JSON_NAME}" if prefix else SUMMARY_JSON_NAME,
            )
            return any(item["Key"].endswith(SUMMARY_JSON_NAME) for item in response.get("Contents", []))
        except Exception:
            return False

    return (Path(output_uri) / SUMMARY_JSON_NAME).exists()


output_uri = st.sidebar.text_input("Output path", value=get_output_uri())
input_uri = st.sidebar.text_input("Input path", value=get_input_uri())

with st.sidebar:
    st.subheader("Pipeline automation")
    full_pipeline_clicked = st.button("Run full pipeline")
    batch_run_clicked = st.button("Run batch layer")
    speed_run_clicked = st.button("Run speed replay")

    if full_pipeline_clicked:
        java_ok, java_output = check_java_runtime()
        if not java_ok:
            st.error("The batch layer requires a Java Runtime. Install OpenJDK 17 and make sure `java -version` works before retrying.")
            st.code(java_output or "Java runtime check failed.")
        else:
            with st.spinner("Running full pipeline…"):
                batch_ok, batch_output = run_command(
                    [
                        sys.executable,
                        "batch/batch_processing.py",
                        "--input",
                        input_uri,
                        "--output",
                        output_uri,
                    ]
                )
                speed_uri = compute_speed_output_uri(output_uri)
                speed_command = [
                    sys.executable,
                    "speed/streaming.py",
                    "--input",
                    input_uri,
                    "--window-size",
                    "5",
                    "--mode",
                    "csv",
                ]
                if speed_uri:
                    speed_command.extend(["--output-s3-uri", speed_uri])
                speed_ok, speed_output = run_command(speed_command)

            log_lines = ["[batch]", batch_output or "No batch output captured."]
            log_lines.append("[speed]" + (f"\n{speed_output}" if speed_output else "\nNo speed output captured."))

            if batch_ok and speed_ok:
                st.success("Full pipeline ran successfully.")
            else:
                st.error("The full pipeline finished with errors.")
            st.code("\n\n".join(log_lines))

    if batch_run_clicked:
        java_ok, java_output = check_java_runtime()
        if not java_ok:
            st.error("The batch layer requires a Java Runtime. Install OpenJDK 17 and make sure `java -version` works before retrying.")
            st.code(java_output or "Java runtime check failed.")
        else:
            with st.spinner("Running batch layer…"):
                batch_ok, batch_output = run_command(
                    [
                        sys.executable,
                        "batch/batch_processing.py",
                        "--input",
                        input_uri,
                        "--output",
                        output_uri,
                    ]
                )
            if batch_ok:
                st.success("Batch layer completed successfully.")
            else:
                st.error("Batch layer failed.")
            st.code(batch_output or "No batch output captured.")

    if speed_run_clicked:
        with st.spinner("Running speed replay…"):
            speed_command = [
                sys.executable,
                "speed/streaming.py",
                "--input",
                input_uri,
                "--window-size",
                "5",
                "--mode",
                "csv",
            ]
            speed_uri = compute_speed_output_uri(output_uri)
            if speed_uri:
                speed_command.extend(["--output-s3-uri", speed_uri])
            speed_ok, speed_output = run_command(speed_command)
        if speed_ok:
            st.success("Speed replay completed successfully.")
        else:
            st.error("Speed replay failed.")
        st.code(speed_output or "No speed output captured.")


def load_json(uri: str, relative_name: str) -> dict:
    if is_s3_uri(uri):
        bucket, prefix = split_s3_uri(uri)
        key = f"{prefix}/{relative_name}" if prefix else relative_name
        response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    with open(Path(uri) / relative_name, encoding="utf-8") as handle:
        return json.load(handle)


def load_parquet_prefix(uri: str, prefix_name: str) -> pd.DataFrame | None:
    if is_s3_uri(uri):
        bucket, prefix = split_s3_uri(uri)
        base_prefix = f"{prefix}/{prefix_name}/" if prefix else f"{prefix_name}/"
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

    target = Path(uri) / prefix_name
    try:
        return pd.read_parquet(target)
    except FileNotFoundError:
        return None


def load_daily_df(uri: str) -> pd.DataFrame | None:
    return load_parquet_prefix(uri, "daily_speed")


def load_disturbance_breakdown(uri: str) -> pd.DataFrame | None:
    return load_parquet_prefix(uri, "disturbance_breakdown")


def load_latest_speed_snapshots(uri: str) -> pd.DataFrame | None:
    if not is_s3_uri(uri):
        return None

    bucket, prefix = split_s3_uri(uri)
    speed_prefix = (
        prefix.replace(BATCH_URI_SUFFIX, LIVE_SPEED_URI_SUFFIX)
        if BATCH_URI_SUFFIX in prefix
        else LIVE_SPEED_URI_SUFFIX.lstrip("/")
    )
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


summary_exists = safe_summary_exists(output_uri)

if not summary_exists:
    st.info("Batch output was not found. Use the sidebar controls to run the pipeline from the dashboard itself.")
    st.stop()

summary = load_json(output_uri, "summary.json")
percentiles = load_json(output_uri, "percentiles.json")

source_col, sink_col = st.columns(2)
source_col.info(f"Input source: {input_uri}")
sink_col.info(f"Output target: {output_uri}")

st.subheader("Lambda Architecture Overview")
overview_cols = st.columns(6)
overview_cols[0].metric("Records", summary["record_count"])
overview_cols[1].metric("Avg Speed", summary["avg_speed"])
overview_cols[2].metric("Max Speed", summary["max_speed"])
overview_cols[3].metric("Min Speed", summary["min_speed"])
overview_cols[4].metric("Avg Disturbance", summary["avg_disturbance_score"])
overview_cols[5].metric("P95 Speed", percentiles["p95"])

with st.expander("Batch layer summary JSON", expanded=False):
    st.json(summary)

with st.expander("Historical percentiles", expanded=True):
    st.json(percentiles)

batch_tabs = st.tabs(["Batch Trends", "Risk Breakdown", "Serving View"])

with batch_tabs[0]:
    daily_df = load_daily_df(output_uri)
    if daily_df is not None and not daily_df.empty:
        daily_df["date"] = pd.to_datetime(daily_df[["year", "month", "day"]])
        fig = px.line(
            daily_df,
            x="date",
            y="daily_avg_speed",
            title="Daily Average Speed (Batch Layer)",
            color_discrete_sequence=["#32d1ff"],
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font={"color": "#eaf8ff"},
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No daily batch parquet outputs were found yet.")

with batch_tabs[1]:
    breakdown_df = load_disturbance_breakdown(output_uri)
    if breakdown_df is not None and not breakdown_df.empty:
        breakdown_df = breakdown_df.sort_values("risk_band")
        risk_fig = px.bar(
            breakdown_df,
            x="risk_band",
            y="event_count",
            title="Disturbance Risk Distribution",
            color="risk_band",
            color_discrete_map={
                "baseline": "#59d98b",
                "elevated": "#ffd166",
                "high": "#ff9f43",
                "extreme": "#ff5d73",
            },
        )
        risk_fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font={"color": "#eaf8ff"},
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(risk_fig, use_container_width=True)
        st.dataframe(breakdown_df, use_container_width=True)
    else:
        st.warning("No disturbance breakdown parquet outputs were found yet.")

with batch_tabs[2]:
    speed_df = load_latest_speed_snapshots(output_uri)
    if speed_df is not None and not speed_df.empty:
        latest = speed_df.iloc[-1]
        st.subheader("Serving View (Batch + Speed Merge)")
        serve_cols = st.columns(4)
        serve_cols[0].metric("Current Speed", latest["current_speed"])
        serve_cols[1].metric("Rolling Avg Speed", latest["rolling_avg_speed"])
        serve_cols[2].metric("Disturbance Score", latest["disturbance_score"])
        serve_cols[3].metric("Risk Band", latest["risk_band"])

        st.dataframe(
            speed_df[["time_tag", "current_speed", "rolling_avg_speed", "disturbance_score", "risk_band"]].tail(10),
            use_container_width=True,
        )

        speed_df["time_tag"] = pd.to_datetime(speed_df["time_tag"])
        live_fig = px.line(
            speed_df,
            x="time_tag",
            y=["current_speed", "rolling_avg_speed"],
            title="Live Speed vs Rolling Average",
            color_discrete_sequence=["#6ee7ff", "#ffd166"],
        )
        live_fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font={"color": "#eaf8ff"},
            margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(live_fig, use_container_width=True)
    else:
        st.warning("No speed-layer snapshots were found yet. Run the speed replay or Kinesis path first.")
