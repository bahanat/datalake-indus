"""
DAG #2 — Transformation vers staging

Objectifs :

- Harmoniser les noms de colonnes
- Ajouter elapsed_time lorsqu'il est absent
- Normaliser les timestamps
- Conserver le même partitionnement que dans raw
- Déposer le résultat dans staging
"""

from __future__ import annotations

import io
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task

from scripts.utils.config import (
    COLUMN_MAPPING,
    LINES,
    RAW_BUCKET,
    STAGING_BUCKET,
    TARGET_COLUMNS,
)

from scripts.utils.minio import (
    get_s3_client,
    list_keys,
    upload_dataframe,
)


def harmonize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Harmonise le schéma des différentes lignes de production.
    """

    # ------------------------------------------------------------------
    # Harmonisation des noms de colonnes
    # ------------------------------------------------------------------

    df = df.rename(columns=COLUMN_MAPPING)

    # ------------------------------------------------------------------
    # elapsed_time est optionnel dans certaines lignes
    # ------------------------------------------------------------------

    if "elapsed_time" not in df.columns:
        df["elapsed_time"] = float("nan")

    # ------------------------------------------------------------------
    # Vérification du schéma
    # ------------------------------------------------------------------

    missing = [column for column in TARGET_COLUMNS if column not in df.columns]

    if missing:
        raise ValueError(f"Colonnes manquantes : {missing}")

    # ------------------------------------------------------------------
    # Normalisation timestamp
    # ------------------------------------------------------------------

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # ------------------------------------------------------------------
    # Réorganisation des colonnes
    # ------------------------------------------------------------------

    df = df[TARGET_COLUMNS]

    return df


@task
def transform_line(line: str) -> dict:
    """
    Transforme tous les objets raw appartenant à une ligne de production.
    """

    s3 = get_s3_client()

    prefix = f"production_lines/{line}/"

    keys = list_keys(
        s3=s3,
        bucket=RAW_BUCKET,
        prefix=prefix,
    )

    if not keys:

        print(f"[{line}] Aucun fichier trouvé.")

        return {
            "line": line,
            "files": 0,
        }

    processed = 0

    for key in keys:

        obj = s3.get_object(
            Bucket=RAW_BUCKET,
            Key=key,
        )

        df = pd.read_csv(io.BytesIO(obj["Body"].read()))

        df = harmonize(df)

        upload_dataframe(
            s3=s3,
            bucket=STAGING_BUCKET,
            key=key,
            df=df,
        )

        processed += 1

        print(f"[{line}] " f"{key} " f"({len(df)} lignes)")

    return {
        "line": line,
        "files": processed,
    }


@task
def log_summary(results: list[dict]) -> None:
    """
    Résumé de la transformation.
    """

    print("\n===================================")
    print("Résumé DAG #2")
    print("===================================\n")

    total = 0

    for result in results:

        total += result["files"]

        print(f"{result['line']:<8}" f"{result['files']} fichier(s)")

    print()

    print(f"Total : {total} fichier(s) transformé(s)")

    print()

    print("Schéma cible :")

    for column in TARGET_COLUMNS:
        print(f" - {column}")


@dag(
    dag_id="dag_02_transformation_staging",
    description="Transformation des données raw vers staging",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "staging",
        "transformation",
        "minio",
    ],
)
def dag_02_transformation():

    tasks = []

    for line in LINES:

        task = transform_line.override(task_id=f"transform_{line}")(line=line)

        tasks.append(task)

    log_summary(tasks)


dag_02_transformation()
