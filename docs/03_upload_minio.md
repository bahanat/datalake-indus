## Déploiement MinIO

MinIO est déployé via Docker Compose avec deux ports exposés :

* `9000` — API S3 (point d'accès boto3)
* `9001` — Console web d'administration

```yaml
services:
  minio:
    image: quay.io/minio/minio
    container_name: minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data

volumes:
  minio-data:
```

<br>

## Création des buckets

Les quatre buckets correspondent aux couches définies dans l'architecture :

| Bucket    | Rôle                              |
|-----------|-----------------------------------|
| `raw`     | Données brutes d'origine          |
| `staging` | Données nettoyées et harmonisées  |
| `curated` | Données prêtes à l'analyse        |
| `archive` | Données expirées (cycle de vie)   |

<br>

## Politiques d'accès

Les politiques d'accès sont différenciées par couche :

| Bucket    | Politique    | Justification                                      |
|-----------|--------------|----------------------------------------------------|
| `raw`     | Lecture/Écriture | Ingestion des fichiers sources                 |
| `staging` | Lecture/Écriture | Transformations et dépôt des fichiers nettoyés |
| `curated` | Lecture seule    | Consommation analytique, pas de modification   |
| `archive` | Lecture seule    | Conservation en audit, pas de modification     |

Ces politiques sont appliquées au niveau bucket via l'API S3. Les politiques par compte de service (data-analyst, data-engineer, admin) seront configurées en semaine 2.

<br>

## Ingestion des fichiers

Les cinq CSV sont déposés dans le bucket `raw` avec un partitionnement temporel extrait de leur contenu :

```text
raw/
└── production_lines/
    ├── lineA/year=2025/month=05/LineA_Stable_10K.csv
    ├── lineB/year=2025/month=04/LineB_Flux.csv
    ├── lineC/year=2025/month=04/LineC_Turbulent.csv
    ├── lineD/year=2025/month=04/LineD_SpikeControl.csv
    └── lineE/year=2025/month=04/LineE_SmoothRun.csv
```

Le partitionnement `year=/month=/` est dérivé automatiquement en lisant le premier horodatage de chaque fichier, sans valeur codée en dur.

<br>

## Vérification d'intégrité

Chaque fichier est contrôlé après dépôt par comparaison de son empreinte MD5 locale avec l'ETag retourné par MinIO.

Pour les fichiers inférieurs à 5 Go déposés en une seule partie, MinIO stocke le MD5 du contenu comme ETag. L'égalité des deux valeurs garantit qu'aucune corruption n'a eu lieu pendant le transfert.

Un fichier `upload_manifest.json` est généré à l'issue du script et conserve pour chaque fichier :

* le chemin dans le bucket
* le partitionnement appliqué
* l'empreinte MD5 locale
* l'ETag distant
* le statut de vérification

<br>

## Script d'ingestion

Le script `scripts/upload_to_minio.py` regroupe les quatre opérations :

1. création des buckets
2. application des politiques d'accès
3. upload avec partitionnement automatique
4. vérification d'intégrité et génération du manifeste