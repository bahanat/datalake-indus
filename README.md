# Datalake Industriel — Maintenance Prédictive

Déploiement d'un data lake moderne pour centraliser, documenter et sécuriser des données issues de capteurs industriels, en vue d'un projet de maintenance prédictive.

**Source :** [Synthetic Data from Industrial Sensor Monitoring](https://zenodo.org/records/15277168) — Polytechnic Institute of Porto / INESC TEC, avril 2025.

---

## Stack technique

| Composant    | Rôle                           |
|--------------|--------------------------------|
| MinIO        | Stockage objet (S3-compatible) |
| Airflow      | Orchestration des pipelines    |
| OpenMetadata | Catalogue de données           |
| boto3        | Ingestion Python               |
| Docker Compose | Infrastructure locale        |

---

## Structure du projet

```text
.
├── architecture/        # Schéma technique (draw.io / PDF)
├── dags/                # DAGs Airflow
│   ├── dag_01_ingestion.py
│   ├── dag_02_transformation.py
│   └── requirements.txt
├── data/
│   └── raw/             # CSV sources (non versionnés)
├── docs/                # Documentation par étape
├── logs/                # Logs Airflow (non versionnés)
├── notebooks/           # Exploration des données
├── scripts/             # Scripts Python
│   └── upload_to_minio.py
└── docker-compose.yml
```

---

## Lancer l'environnement

```bash
# Premier lancement uniquement — permissions logs Airflow
sudo chown -R 50000:0 logs/

docker compose up -d
```

| Interface      | URL                       | Identifiants      |
|----------------|---------------------------|-------------------|
| MinIO console  | http://localhost:9001     | minioadmin / minioadmin |
| Airflow UI     | http://localhost:8080     | admin / admin     |

---

## Ingestion manuelle (jour 2)

Upload des 5 CSV bruts dans `raw/` sans partitionnement :

```bash
pip install boto3 pandas
python scripts/upload_to_minio.py
```

---

## Pipelines Airflow (jours 3-4)

Les DAGs sont déclenchés manuellement depuis `http://localhost:8080` dans l'ordre suivant :

1. `dag_01_ingestion_` — dépose les CSV dans `raw/` avec partitionnement `year=/month=/`
2. `dag_02_transformation` — harmonise les schémas et dépose en `staging/`

---

## Documentation

| Fichier | Contenu |
|---------|---------|
| `docs/01_data_exploration.md` | Analyse des CSV, hétérogénéité des schémas |
| `docs/02_data_modelisation.md` | Architecture en couches, schéma cible |
| `docs/03_minio_setup_ingestion.md` | Déploiement MinIO, buckets, politiques, ingestion |
| `docs/04_airflow_raw_staging.md` | Infrastructure Airflow, DAG #1 et DAG #2 |