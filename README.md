# Datalake Industriel — Maintenance Prédictive

Déploiement d'un data lake moderne pour centraliser, documenter et sécuriser des données issues de capteurs industriels, en vue d'un projet de maintenance prédictive.

**Source :** [Synthetic Data from Industrial Sensor Monitoring](https://zenodo.org/records/15277168) — Polytechnic Institute of Porto / INESC TEC, avril 2025.

---

## Stack technique

| Composant     | Rôle                        |
|---------------|-----------------------------|
| MinIO         | Stockage objet (S3-compatible) |
| Airflow       | Orchestration des pipelines |
| OpenMetadata  | Catalogue de données        |
| boto3         | Ingestion Python            |
| Docker Compose| Infrastructure locale       |

---

## Structure du projet

```text
.
├── architecture/        # Schéma technique (draw.io / PDF)
├── data/
│   └── raw/             # CSV sources (non versionnés)
├── docs/                # Documentation par étape
├── notebooks/           # Exploration des données
├── scripts/             # Scripts Python
│   └── upload_to_minio.py
└── docker-compose.yml
```

---

## Lancer l'environnement

```bash
docker compose up -d
```

Console MinIO disponible sur `http://localhost:9001` — identifiants par défaut : `minioadmin / minioadmin`.

---

## Ingestion des données

```bash
pip install boto3 pandas
python scripts/upload_to_minio.py
```

Dépose les 5 CSV dans le bucket `raw` avec partitionnement `year=/month=/line=/` et vérifie l'intégrité par MD5.

---

## Documentation

| Fichier | Contenu |
|---------|---------|
| `docs/01_data_exploration.md` | Analyse des CSV, hétérogénéité des schémas |
| `docs/02_data_modelisation.md` | Architecture en couches, schéma cible |
| `docs/03_minio_setup_ingestion.md` | Déploiement MinIO, buckets, politiques, ingestion |