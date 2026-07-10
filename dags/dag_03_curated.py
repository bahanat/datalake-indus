"""
DAG #3 — Création de la couche curated

STAGING
    |
    v
CURATED

Transformation :
- CSV -> Parquet
- ajout production_line
- ajout ingestion_date
- contrôle qualité minimal
"""

from datetime import datetime
import io

import pandas as pd

from airflow.decorators import dag, task


from scripts.utils.config import (
    LINES,
    STAGING_BUCKET,
    CURATED_BUCKET,
)

from scripts.utils.minio import (
    get_s3_client,
    list_keys,
)


@task
def create_curated(line: str) -> dict:

    s3 = get_s3_client()

    prefix = f"production_lines/{line}/"

    keys = list_keys(
        s3,
        STAGING_BUCKET,
        prefix,
    )

    if not keys:
        return {"line": line, "files": 0}

    parquet_files = 0

    for key in keys:

        obj = s3.get_object(
            Bucket=STAGING_BUCKET,
            Key=key,
        )

        df = pd.read_csv(io.BytesIO(obj["Body"].read()))

        # -----------------------------
        # Qualité données
        # -----------------------------

        df = df.drop_duplicates()

        df["production_line"] = line

        df["ingestion_date"] = datetime.utcnow().date()

        required = [
            "timestamp",
            "temperature",
            "pressure",
            "label",
        ]

        missing = [c for c in required if c not in df.columns]

        if missing:
            raise ValueError(f"{line} colonnes absentes {missing}")

        # -----------------------------
        # CSV -> Parquet
        # -----------------------------

        buffer = io.BytesIO()

        df.to_parquet(
            buffer,
            index=False,
            engine="pyarrow",
        )

        curated_key = key.replace(".csv", ".parquet")

        s3.put_object(
            Bucket=CURATED_BUCKET,
            Key=curated_key,
            Body=buffer.getvalue(),
        )

        parquet_files += 1

        print(f"{line} : {curated_key}")

    return {
        "line": line,
        "files": parquet_files,
    }


@task
def summary(results):

    print("=== CURATED SUMMARY ===")

    for r in results:
        print(r)


@dag(
    dag_id="dag_03_create_curated",
    description="Création couche curated Parquet",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["curated", "parquet", "quality"],
)
def dag_03_curated():

    tasks = []

    for line in LINES:

        tasks.append(create_curated(line))

    summary(tasks)


dag_03_curated()
