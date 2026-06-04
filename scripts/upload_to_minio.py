import hashlib
import json
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# ── Config ────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS   = "minioadmin"
MINIO_SECRET   = "minioadmin"

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

LINE_MAP = {
    "LineA_Stable_10K.csv":  "lineA",
    "LineB_Flux.csv":         "lineB",
    "LineC_Turbulent.csv":    "lineC",
    "LineD_SpikeControl.csv": "lineD",
    "LineE_SmoothRun.csv":    "lineE",
}

BUCKETS = ["raw", "staging", "curated", "archive"]

# ── Policies ──────────────────────────────────────────────────────────────────

def rw_policy(bucket: str) -> str:
    return json.dumps({
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
    })


def ro_policy(bucket: str) -> str:
    return json.dumps({
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
    })


BUCKET_POLICIES = {
    "raw":     rw_policy("raw"),
    "staging": rw_policy("staging"),
    "curated": ro_policy("curated"),
    "archive": ro_policy("archive"),
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )

    # 1. Création des buckets --------------------------------------------------
    print("=== Création des buckets ===")
    for bucket in BUCKETS:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"  ✓ Créé     : {bucket}")
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                print(f"  ~ Existant : {bucket}")
            else:
                raise

    # 2. Application des policies ----------------------------------------------
    print("\n=== Policies d'accès ===")
    for bucket, policy in BUCKET_POLICIES.items():
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)
        access = "lecture/écriture" if bucket in ("raw", "staging") else "lecture seule"
        print(f"  ✓ {bucket:<10} → {access}")

    # 3. Upload des CSV --------------------------------------------------------
    # Chemin cible : raw/production_lines/lineX/fichier.csv
    # Le partitionnement year=/month=/ sera ajouté par Airflow (jours 3-4)
    print("\n=== Upload vers raw/ ===")
    results = []

    for filename, line_label in LINE_MAP.items():
        local_path = DATA_DIR / filename

        if not local_path.exists():
            print(f"  ✗ Fichier introuvable : {local_path}")
            continue

        key = f"production_lines/{line_label}/{filename}"
        local_md5 = md5(local_path)

        print(f"\n  [{line_label}]")
        print(f"    fichier : {filename}")
        print(f"    clé     : raw/{key}")
        print(f"    MD5     : {local_md5}")

        s3.upload_file(str(local_path), "raw", key)

        # Vérification intégrité : ETag MinIO == MD5 pour upload < 5 Go
        head = s3.head_object(Bucket="raw", Key=key)
        remote_etag = head["ETag"].strip('"')

        if remote_etag == local_md5:
            print(f"    ✓ Intégrité OK")
            status = "OK"
        else:
            print(f"    ✗ ÉCHEC — local={local_md5}  distant={remote_etag}")
            status = "MISMATCH"

        results.append({
            "fichier":      filename,
            "ligne":        line_label,
            "bucket_key":   f"raw/{key}",
            "md5_local":    local_md5,
            "etag_distant": remote_etag,
            "statut":       status,
        })

    # 4. Résumé ----------------------------------------------------------------
    print("\n=== Résumé ===")
    ok  = sum(1 for r in results if r["statut"] == "OK")
    err = len(results) - ok
    print(f"  Uploadés : {ok}/{len(results)}")
    if err:
        print(f"  Erreurs  : {err} fichier(s) — vérifier les lignes ci-dessus")
    else:
        print("  Tous les checksums sont valides ✓")

    manifest_path = Path(__file__).parent.parent / "upload_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n  Manifeste écrit → {manifest_path}")


if __name__ == "__main__":
    main()