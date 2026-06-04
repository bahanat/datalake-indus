"""
DAG #1 — Ingestion brute
Dépose chaque CSV dans raw/ avec partitionnement year=/month=/line=/
LineA est traitée en chunks de 1000 lignes pour simuler un flux réel.
"""

from __future__ import annotations

import hashlib
import io
import os
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
from airflow.decorators import dag, task

# ── Config ────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_RAW      = "raw"
DATA_DIR        = Path("/opt/airflow/data/raw")
CHUNK_SIZE      = 1000  # lignes par batch pour LineA

LINE_MAP = {
    "lineA": "LineA_Stable_10K.csv",
    "lineB": "LineB_Flux.csv",
    "lineC": "LineC_Turbulent.csv",
    "lineD": "LineD_SpikeControl.csv",
    "lineE": "LineE_SmoothRun.csv",
}


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )


def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def get_partition(df: pd.DataFrame) -> tuple[str, str]:
    """Extrait year et month depuis la première ligne du dataframe."""
    ts_col = next(c for c in df.columns if c.lower() == "timestamp")
    ts = pd.to_datetime(df[ts_col].iloc[0])
    return str(ts.year), f"{ts.month:02d}"


def build_key(line: str, year: str, month: str, filename: str) -> str:
    return f"production_lines/{line}/year={year}/month={month}/{filename}"


# ── Tasks ─────────────────────────────────────────────────────────────────────

@task
def ingest_line(line: str, filename: str) -> dict:
    """Ingère un CSV vers raw/ avec partitionnement temporel."""
    s3 = get_s3_client()
    local_path = DATA_DIR / filename

    df = pd.read_csv(local_path)
    year, month = get_partition(df)

    if line == "lineA":
        # Traitement par chunks pour simuler un flux réel
        results = []
        for i, start in enumerate(range(0, len(df), CHUNK_SIZE)):
            chunk = df.iloc[start:start + CHUNK_SIZE]
            chunk_filename = filename.replace(".csv", f"_chunk_{i:03d}.csv")
            key = build_key(line, year, month, chunk_filename)

            buf = io.BytesIO()
            chunk.to_csv(buf, index=False)
            data = buf.getvalue()

            s3.put_object(Bucket=BUCKET_RAW, Key=key, Body=data)
            results.append({
                "key": key,
                "lignes": len(chunk),
                "md5": md5_bytes(data),
            })

        print(f"[{line}] {len(results)} chunks uploadés ({len(df)} lignes total)")
        return {"line": line, "mode": "chunks", "chunks": results}

    else:
        # Upload direct pour les autres lignes
        key = build_key(line, year, month, filename)
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        data = buf.getvalue()

        s3.put_object(Bucket=BUCKET_RAW, Key=key, Body=data)
        checksum = md5_bytes(data)

        print(f"[{line}] Uploadé → raw/{key} (MD5: {checksum})")
        return {"line": line, "mode": "direct", "key": key, "md5": checksum}


@task
def log_summary(results: list[dict]) -> None:
    """Affiche un résumé de l'ingestion."""
    print("\n=== Résumé DAG #1 — Ingestion ===")
    for r in results:
        if r["mode"] == "chunks":
            total = sum(c["lignes"] for c in r["chunks"])
            print(f"  {r['line']:<8} → {len(r['chunks'])} chunks ({total} lignes)")
        else:
            print(f"  {r['line']:<8} → {r['key']}")


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="dag_01_ingestion_raw",
    description="Ingestion des CSV vers raw/ avec partitionnement temporel",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ingestion", "raw"],
)
def dag_01_ingestion():
    ingestion_tasks = [
        ingest_line.override(task_id=f"ingest_{line}")(line=line, filename=filename)
        for line, filename in LINE_MAP.items()
    ]
    log_summary(ingestion_tasks)


dag_01_ingestion()