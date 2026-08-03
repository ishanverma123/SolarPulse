from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark SolarPulse batch processing sequentially vs Spark parallel.")
    parser.add_argument("--input", required=True, help="Historical CSV input path.")
    return parser.parse_args()


def run_command(command: list[str]) -> dict:
    start = time.perf_counter()
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    elapsed = time.perf_counter() - start
    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()

    payload: dict = {}
    if stdout:
        try:
            payload = ast.literal_eval(stdout)
        except (SyntaxError, ValueError):
            try:
                payload = json.loads(stdout)
            except json.JSONDecodeError:
                payload = {"raw_output": stdout}

    if completed.returncode != 0:
        payload["return_code"] = completed.returncode
        payload["stderr"] = stderr

    payload["elapsed_seconds"] = round(elapsed, 6)
    return payload


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    sequential = run_command([sys.executable, "benchmark/sequential.py", "--input", args.input])
    parallel = run_command([sys.executable, "benchmark/parallel.py", "--input", args.input])

    seq_elapsed = float(sequential.get("elapsed_seconds", 0.0))
    par_elapsed = float(parallel.get("elapsed_seconds", 0.0))
    speedup = (seq_elapsed / par_elapsed) if par_elapsed > 0 else 0.0

    report = {
        "input": args.input,
        "sequential": sequential,
        "parallel": parallel,
        "speedup_ratio": round(speedup, 3),
        "parallel_is_faster": par_elapsed < seq_elapsed,
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
