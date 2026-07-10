"""
DAG #1 — Ingestion brute

Dépose chaque CSV dans raw/ avec partitionnement :

production_lines/
    lineX/
        year=YYYY/
            month=MM/

La ligne A est traitée par chunks de 1000 lignes afin de simuler un flux
d'ingestion progressif.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task

from scripts.utils.config import (
    AIRFLOW_DATA,
    CHUNK_SIZE,
    LINE_MAP,
    RAW_BUCKET,
)

from scripts.utils.minio import (
    build_key,
    dataframe_to_bytes,
    get_partition,
    get_s3_client,
    md5_bytes,
)


@task
def ingest_line(line: str, filename: str) -> dict:
    """
    Ingère un CSV dans le bucket raw.
    """

    s3 = get_s3_client()

    local_path = AIRFLOW_DATA / filename

    if not local_path.exists():
        raise FileNotFoundError(local_path)

    df = pd.read_csv(local_path)

    year, month = get_partition(df)

    # ------------------------------------------------------------------
    # Line A : simulation d'un flux via des chunks
    # ------------------------------------------------------------------

    if line == "lineA":

        uploaded_chunks = []

        for i, start in enumerate(range(0, len(df), CHUNK_SIZE)):

            chunk = df.iloc[start : start + CHUNK_SIZE]

            chunk_filename = filename.replace(
                ".csv",
                f"_chunk_{i:03d}.csv",
            )

            key = build_key(
                line=line,
                year=year,
                month=month,
                filename=chunk_filename,
            )

            data = dataframe_to_bytes(chunk)

            s3.put_object(
                Bucket=RAW_BUCKET,
                Key=key,
                Body=data,
            )

            uploaded_chunks.append(
                {
                    "key": key,
                    "rows": len(chunk),
                    "md5": md5_bytes(data),
                }
            )

            print(f"[{line}] " f"chunk {i:03d} " f"({len(chunk)} lignes)")

        print(
            f"[{line}] "
            f"{len(uploaded_chunks)} chunks uploadés "
            f"({len(df)} lignes)"
        )

        return {
            "line": line,
            "mode": "chunks",
            "chunks": uploaded_chunks,
        }

    # ------------------------------------------------------------------
    # Autres lignes
    # ------------------------------------------------------------------

    key = build_key(
        line=line,
        year=year,
        month=month,
        filename=filename,
    )

    data = dataframe_to_bytes(df)

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=data,
    )

    checksum = md5_bytes(data)

    print(f"[{line}] " f"Upload terminé : " f"{key}")

    print(f"MD5 : {checksum}")

    return {
        "line": line,
        "mode": "direct",
        "key": key,
        "md5": checksum,
    }


@task
def log_summary(results: list[dict]) -> None:
    """
    Résumé de l'ingestion.
    """

    print("\n===================================")
    print("Résumé DAG #1")
    print("===================================\n")

    for result in results:

        if result["mode"] == "chunks":

            total_rows = sum(chunk["rows"] for chunk in result["chunks"])

            print(
                f"{result['line']:<8}"
                f"{len(result['chunks'])} chunks"
                f" ({total_rows} lignes)"
            )

        else:

            print(f"{result['line']:<8}" f"{result['key']}")


@dag(
    dag_id="dag_01_ingestion_raw",
    description="Ingestion des CSV vers le bucket raw",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "raw",
        "ingestion",
        "minio",
    ],
)
def dag_01_ingestion():

    tasks = []

    for line, filename in LINE_MAP.items():

        task = ingest_line.override(task_id=f"ingest_{line}")(
            line=line,
            filename=filename,
        )

        tasks.append(task)

    log_summary(tasks)


dag_01_ingestion()
