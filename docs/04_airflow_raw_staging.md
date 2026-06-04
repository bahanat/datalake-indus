## Infrastructure Airflow

Airflow est ajouté au Docker Compose existant. Il repose sur trois services :

| Service              | Rôle                                                  |
|----------------------|-------------------------------------------------------|
| `postgres`           | Base de données interne Airflow (état, logs, runs)    |
| `airflow-init`       | Migration BDD et création du compte admin (one-shot)  |
| `airflow-scheduler`  | Planification et exécution des tâches                 |
| `airflow-webserver`  | Interface web — `http://localhost:8080`               |

PostgreSQL est requis car les DAGs utilisent le `LocalExecutor`, qui permet d'exécuter les tâches des 5 lignes en parallèle. SQLite, l'alternative plus simple, ne supporte que le `SequentialExecutor` (une tâche à la fois).

Les credentials MinIO sont injectés via variables d'environnement dans le compose et lus dans les DAGs via `os.environ` — ils ne sont pas codés en dur dans le code.

### Démarrage

```bash
# Correction des permissions nécessaire au premier lancement
sudo chown -R 50000:0 logs/

docker compose up -d
docker compose logs -f airflow-init  # attendre "Admin user admin created"
```

Interface disponible sur `http://localhost:8080` — identifiants `admin / admin`.

<br>

## DAG #1 — Ingestion brute

Le DAG `dag_01_ingestion_raw` dépose chaque CSV dans le bucket `raw/` avec partitionnement temporel extrait du contenu des fichiers.

### Structure cible

```text
raw/
└── production_lines/
    ├── lineA/year=2025/month=05/LineA_Stable_10K_chunk_000.csv
    │                            LineA_Stable_10K_chunk_001.csv
    │                            ...
    ├── lineB/year=2025/month=04/LineB_Flux.csv
    ├── lineC/year=2025/month=03/LineC_Turbulent.csv
    ├── lineD/year=2025/month=02/LineD_SpikeControl.csv
    └── lineE/year=2025/month=01/LineE_SmoothRun.csv
```

### Traitement de LineA par chunks

LineA contient 10 000 enregistrements, contre ~5 000 pour les autres lignes. Elle est découpée en chunks de 1 000 lignes pour simuler un flux d'ingestion incrémentale :

* chaque chunk est uploadé séparément sous un nom distinct (`_chunk_000.csv`, `_chunk_001.csv`, …)
* cela représente 10 fichiers déposés séquentiellement
* les autres lignes sont uploadées en un seul fichier

### Vérification d'intégrité

Chaque fichier uploadé fait l'objet d'un calcul MD5 côté client, journalisé dans les logs de la tâche.

<br>

## DAG #2 — Transformation vers staging

Le DAG `dag_02_transformation_staging` lit l'ensemble des fichiers présents dans `raw/` pour chaque ligne, applique les transformations et dépose le résultat dans `staging/` en conservant le même partitionnement.

### Transformations appliquées

| Opération | Détail |
|-----------|--------|
| Normalisation des colonnes | Tous les noms convertis en minuscules |
| Ajout de `elapsed_time` | Colonne absente sur LineC, D, E — complétée avec `NaN` |
| Normalisation du timestamp | Conversion en `datetime` pandas |
| Réordonnancement | Schéma cible appliqué à toutes les lignes |

### Schéma cible

| Colonne        | Type     |
|----------------|----------|
| `timestamp`    | datetime |
| `temperature`  | float    |
| `pressure`     | float    |
| `elapsed_time` | float    |
| `label`        | integer  |

### Chaînage des DAGs

Les deux DAGs sont indépendants et déclenchés manuellement depuis l'interface Airflow (`schedule=None`). Le DAG #2 s'appuie sur ce que le DAG #1 a déposé — ils doivent donc être exécutés dans l'ordre.

<br>

## Structure des fichiers

```text
gh/
├── docker-compose.yml
├── dags/
│   ├── dag_01_ingestion.py
│   ├── dag_02_transformation.py
│   └── requirements.txt
└── logs/                     ← monté par Airflow (permissions uid 50000)
```