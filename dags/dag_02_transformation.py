"""
DAG #2 — Transformation vers staging/
- Harmonise les noms de colonnes (tout en minuscules)
- Ajoute elapsed_time=NaN pour les lignes qui ne l'ont pas
- Normalise le timestamp en datetime
- Dépose le résultat dans staging/ avec le même partitionnement
"""

from __future__ import annotations

import io
import os
from datetime import datetime

import boto3
import pandas as pd
from airflow.decorators import dag, task

# ── Config ────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS    = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_RAW      = "raw"
BUCKET_STAGING  = "staging"

# Schéma cible
TARGET_COLUMNS = ["timestamp", "temperature", "pressure", "elapsed_time", "label"]

LINES = ["lineA", "lineB", "lineC", "lineD", "lineE"]


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )


def harmonize(df: pd.DataFrame) -> pd.DataFrame:
    """Applique toutes les transformations de normalisation."""

    # 1. Noms de colonnes en minuscules
    df.columns = [c.lower() for c in df.columns]

    # 2. Timestamp en datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # 3. Ajouter elapsed_time si absent
    if "elapsed_time" not in df.columns:
        df["elapsed_time"] = float("nan")

    # 4. Réordonner selon le schéma cible
    df = df[TARGET_COLUMNS]

    return df


def list_raw_keys(s3, line: str) -> list[str]:
    """Liste tous les objets dans raw/production_lines/{line}/"""
    prefix = f"production_lines/{line}/"
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET_RAW, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


# ── Tasks ─────────────────────────────────────────────────────────────────────

@task
def transform_line(line: str) -> dict:
    """Lit les fichiers raw d'une ligne, les harmonise et les dépose en staging."""
    s3 = get_s3_client()
    keys = list_raw_keys(s3, line)

    if not keys:
        print(f"[{line}] Aucun fichier trouvé dans raw/")
        return {"line": line, "fichiers": 0}

    processed = 0
    for key in keys:
        # Lecture depuis raw/
        obj = s3.get_object(Bucket=BUCKET_RAW, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))

        # Transformation
        df = harmonize(df)

        # Dépôt en staging/ avec le même chemin relatif
        staging_key = key  # même partitionnement, même nom
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        s3.put_object(Bucket=BUCKET_STAGING, Key=staging_key, Body=buf.getvalue())

        print(f"[{line}] {key} → staging/{staging_key} ({len(df)} lignes)")
        processed += 1

    return {"line": line, "fichiers": processed}


@task
def log_summary(results: list[dict]) -> None:
    """Affiche un résumé de la transformation."""
    print("\n=== Résumé DAG #2 — Transformation ===")
    for r in results:
        print(f"  {r['line']:<8} → {r['fichiers']} fichier(s) déposé(s) en staging/")
    print(f"\nSchéma cible appliqué : {TARGET_COLUMNS}")


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="dag_02_transformation_staging",
    description="Harmonisation des schémas et dépôt en staging/",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["transformation", "staging"],
)
def dag_02_transformation():
    transform_tasks = [
        transform_line.override(task_id=f"transform_{line}")(line=line)
        for line in LINES
    ]
    log_summary(transform_tasks)


dag_02_transformation()