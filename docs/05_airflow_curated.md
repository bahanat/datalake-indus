## DAG #3 — Création de la couche curated

Le DAG `dag_03_create_curated` lit l'ensemble des fichiers présents dans `staging/` pour chaque ligne, applique un contrôle qualité minimal, enrichit les données et dépose le résultat dans `curated/` au format Parquet.

### Transformations appliquées

| Opération | Détail |
|-----------|--------|
| Dédoublonnage | Suppression des lignes strictement identiques (`drop_duplicates`) |
| Ajout de `production_line` | Identifiant de la ligne de production (lineA, lineB, ...) |
| Ajout de `ingestion_date` | Date UTC du traitement, pour traçabilité |
| Contrôle des colonnes requises | Vérifie la présence de `timestamp`, `temperature`, `pressure`, `label` — lève une erreur si l'une d'elles manque |
| Conversion CSV → Parquet | Écriture via `pandas.to_parquet` (moteur `pyarrow`) |

### Structure cible

```text
curated/
└── production_lines/
    ├── lineA/year=2025/month=05/LineA_Stable_10K_chunk_000.parquet
    │                            LineA_Stable_10K_chunk_001.parquet
    │                            ...
    ├── lineB/year=2025/month=04/LineB_Flux.parquet
    ├── lineC/year=2025/month=03/LineC_Turbulent.parquet
    ├── lineD/year=2025/month=02/LineD_SpikeControl.parquet
    └── lineE/year=2025/month=01/LineE_SmoothRun.parquet
```

Le partitionnement `year=/month=/` déposé par le DAG #1 est conservé — seul le format et l'extension changent (`.csv` → `.parquet`).

### Pourquoi Parquet

Le format Parquet est retenu pour la couche curated car il correspond à l'usage attendu de cette couche (lecture analytique) :

* stockage colonnaire, plus performant pour les requêtes analytiques que le CSV ligne par ligne
* compression native, réduisant le volume stocké
* typage préservé (contrairement au CSV, entièrement textuel)

### Qualité des données

Le contrôle qualité reste minimal à ce stade : dédoublonnage et vérification de la présence des colonnes obligatoires. Toute ligne de production dont le staging ne contient pas les colonnes `timestamp`, `temperature`, `pressure` ou `label` fait échouer la tâche correspondante (`ValueError`), ce qui empêche un jeu de données incomplet d'atteindre la couche curated.

### Chaînage des DAGs

Comme les DAGs #1 et #2, le DAG #3 est déclenché manuellement (`schedule=None`) et dépend du contenu déposé par le DAG #2 dans `staging/`. L'ordre d'exécution reste :

```text
dag_01_ingestion_raw → dag_02_transformation_staging → dag_03_create_curated
```

### Résumé d'exécution

Une tâche `summary` agrège, pour chaque ligne de production, le nombre de fichiers Parquet générés, et l'affiche dans les logs Airflow à l'issue du DAG.

<br>

## Structure des fichiers (mise à jour)

```text
gh/
├── docker-compose.yml
├── dags/
│   ├── dag_01_ingestion.py
│   ├── dag_02_transformation.py
│   ├── dag_03_curated.py
│   └── requirements.txt
└── logs/                     ← monté par Airflow (permissions uid 50000)
```
