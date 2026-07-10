"""
Fonctions utilitaires MinIO.
"""

import hashlib
import io

import boto3
import pandas as pd

from scripts.utils.config import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)


def get_s3_client():

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def md5_bytes(data: bytes) -> str:

    return hashlib.md5(data).hexdigest()


def dataframe_to_bytes(df: pd.DataFrame) -> bytes:

    buffer = io.BytesIO()

    df.to_csv(
        buffer,
        index=False,
    )

    return buffer.getvalue()


def get_partition(df: pd.DataFrame):

    timestamp_col = next(
        c
        for c in df.columns
        if c.lower() == "timestamp"
    )

    ts = pd.to_datetime(df[timestamp_col].iloc[0])

    return str(ts.year), f"{ts.month:02d}"


def build_key(
    line: str,
    year: str,
    month: str,
    filename: str,
):

    return (
        f"production_lines/"
        f"{line}/"
        f"year={year}/"
        f"month={month}/"
        f"{filename}"
    )


def list_keys(
    s3,
    bucket: str,
    prefix: str,
):

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    keys = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    ):

        for obj in page.get(
            "Contents",
            [],
        ):
            keys.append(obj["Key"])

    return keys


def upload_dataframe(
    s3,
    bucket: str,
    key: str,
    df: pd.DataFrame,
):

    data = dataframe_to_bytes(df)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
    )

    return md5_bytes(data)