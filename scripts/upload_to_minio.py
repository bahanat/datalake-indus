import hashlib
import json
import os
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError

# ── Config ────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin"

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

# Mapping des noms de fichiers
LINE_MAP = {
    "LineA_Stable_10K.csv": "lineA",
    "LineB_Flux.csv": "lineB",
    "LineC_Turbulent.csv": "lineC",
    "LineD_SpikeControl.csv": "lineD",
    "LineE_SmoothRun.csv": "lineE",
}

BUCKETS = ["raw", "staging", "curated", "archive"]

# Modèles de policies (lecture/écriture)
# raw / staging  → read-write  (data engineers)
# curated        → read-only   (analysts)
# archive        → read-only   (audit / compliance)


def rw_policy(bucket: str) -> str:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/*"],
                },
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket}"],
                },
            ],
        }
    )


def ro_policy(bucket: str) -> str:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket}/*"],
                },
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{bucket}"],
                },
            ],
        }
    )


BUCKET_POLICIES = {
    "raw": rw_policy("raw"),
    "staging": rw_policy("staging"),
    "curated": ro_policy("curated"),
    "archive": ro_policy("archive"),
}

# Utils


def md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def get_partition(csv_path: Path) -> tuple[str, str]:
    """Read the first timestamp row and return (year, zero-padded month)."""
    df = pd.read_csv(csv_path, nrows=1)
    # Normalise column name regardless of casing
    ts_col = next(c for c in df.columns if c.lower() == "timestamp")
    ts = pd.to_datetime(df[ts_col].iloc[0])
    return str(ts.year), f"{ts.month:02d}"


def s3_object_key(line_label: str, year: str, month: str, filename: str) -> str:
    return f"production_lines/{line_label}/year={year}/month={month}/{filename}"


# Main


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )

    # 1. Création des buckets
    print("=== Creating buckets ===")
    for bucket in BUCKETS:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"  ✓ Created  : {bucket}")
        except ClientError as e:
            if e.response["Error"]["Code"] in (
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            ):
                print(f"  ~ Exists   : {bucket}")
            else:
                raise

    # 2. Application des policies
    print("\n=== Applying bucket policies ===")
    for bucket, policy in BUCKET_POLICIES.items():
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)
        access = "read-write" if bucket in ("raw", "staging") else "read-only"
        print(f"  ✓ {bucket:<10} → {access}")

    # 3. Upload des CSV et check d'intégrité
    print("\n=== Uploading CSVs to raw/ ===")
    results = []

    for filename, line_label in LINE_MAP.items():
        local_path = DATA_DIR / filename

        if not local_path.exists():
            print(f"  ✗ Not found: {local_path}")
            continue

        year, month = get_partition(local_path)
        key = s3_object_key(line_label, year, month, filename)
        local_md5 = md5(local_path)

        print(f"\n  [{line_label}]")
        print(f"    local  : {local_path.name}")
        print(f"    key    : raw/{key}")
        print(f"    MD5    : {local_md5}")

        # Upload
        s3.upload_file(str(local_path), "raw", key)

        # Verif
        head = s3.head_object(Bucket="raw", Key=key)
        remote_etag = head["ETag"].strip('"')

        if remote_etag == local_md5:
            print(f"    ✓ Integrity OK (ETag matches MD5)")
            status = "OK"
        else:
            print(f"    ✗ MISMATCH — local={local_md5}  remote={remote_etag}")
            status = "MISMATCH"

        results.append(
            {
                "file": filename,
                "line": line_label,
                "bucket_key": f"raw/{key}",
                "year": year,
                "month": month,
                "local_md5": local_md5,
                "remote_etag": remote_etag,
                "status": status,
            }
        )

    # 4. Résumé
    print("\n=== Summary ===")
    ok = sum(1 for r in results if r["status"] == "OK")
    err = len(results) - ok
    print(f"  Uploaded : {ok}/{len(results)}")
    if err:
        print(f"  Errors   : {err} file(s) with integrity mismatch — check above")
    else:
        print("  All checksums verified ✓")

    manifest_path = Path(__file__).parent / "upload_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Manifest written → {manifest_path}")


if __name__ == "__main__":
    main()
