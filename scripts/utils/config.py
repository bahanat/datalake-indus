"""
Configuration centralisée du projet.
"""

from pathlib import Path

# ----------------------------------------------------------------------
# MinIO
# ----------------------------------------------------------------------

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# ----------------------------------------------------------------------
# Buckets
# ----------------------------------------------------------------------

RAW_BUCKET = "raw"
STAGING_BUCKET = "staging"
CURATED_BUCKET = "curated"
ARCHIVE_BUCKET = "archive"

# ----------------------------------------------------------------------
# Données
# ----------------------------------------------------------------------

AIRFLOW_DATA = Path("/opt/airflow/data/raw")

LOCAL_DATA = (
    Path(__file__).resolve()
    .parents[2]
    / "data"
    / "raw"
)

# ----------------------------------------------------------------------
# CSV
# ----------------------------------------------------------------------

LINE_MAP = {
    "lineA": "LineA_Stable_10K.csv",
    "lineB": "LineB_Flux.csv",
    "lineC": "LineC_Turbulent.csv",
    "lineD": "LineD_SpikeControl.csv",
    "lineE": "LineE_SmoothRun.csv",
}

LINES = list(LINE_MAP.keys())

# ----------------------------------------------------------------------
# Harmonisation
# ----------------------------------------------------------------------

TARGET_COLUMNS = [
    "timestamp",
    "temperature",
    "pressure",
    "elapsed_time",
    "label",
]

COLUMN_MAPPING = {
    "Timestamp": "timestamp",
    "timestamp": "timestamp",

    "Temperature": "temperature",
    "temperature": "temperature",

    "Pressure": "pressure",
    "pressure": "pressure",

    "Elapsed_Time": "elapsed_time",
    "elapsed_time": "elapsed_time",

    "Label": "label",
    "label": "label",
}

CHUNK_SIZE = 1000