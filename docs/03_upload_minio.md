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

Des politiques d'accès sont définies au niveau des buckets dès cette étape :

| Bucket    | Politique        |
|-----------|------------------|
| `raw`     | Lecture/Écriture |
| `staging` | Lecture/Écriture |
| `curated` | Lecture seule    |
| `archive` | Lecture seule    |

### Limitation actuelle

Ces politiques s'appliquent aux accès anonymes (sans credentials). À ce stade, elles n'ont pas d'effet opérationnel concret : l'utilisateur root `minioadmin` dispose de tous les droits indépendamment de ces règles.

Leur rôle est de poser le cadre d'accès par couche, en cohérence avec l'architecture définie au jour 1 :

* `raw` et `staging` sont des zones de travail — les pipelines doivent pouvoir y écrire
* `curated` et `archive` sont des zones de consommation — la modification y est interdite

### Évolution prévue

Les politiques par rôle seront configurées au jour 6 via la création de trois comptes de service (`data-analyst`, `data-engineer`, `admin`) avec des policies IAM attachées aux utilisateurs. C'est à ce moment que la différenciation des droits prendra son effet réel.

<br>

## Ingestion des fichiers

Les cinq CSV sont déposés dans le bucket `raw` sans transformation ni partitionnement temporel :

```text
raw/
└── production_lines/
    ├── lineA/LineA_Stable_10K.csv
    ├── lineB/LineB_Flux.csv
    ├── lineC/LineC_Turbulent.csv
    ├── lineD/LineD_SpikeControl.csv
    └── lineE/LineE_SmoothRun.csv
```

Le partitionnement `year=/month=/` sera ajouté par les DAGs Airflow lors de l'automatisation de l'ingestion (jours 3-4).

<br>

## Vérification d'intégrité

Chaque fichier est contrôlé après dépôt par comparaison de son empreinte MD5 locale avec l'ETag retourné par MinIO.

Pour les fichiers inférieurs à 5 Go déposés en une seule partie, MinIO stocke le MD5 du contenu comme ETag. L'égalité des deux valeurs garantit qu'aucune corruption n'a eu lieu pendant le transfert.

Un fichier `upload_manifest.json` est généré à l'issue du script et conserve pour chaque fichier :

* le chemin dans le bucket
* l'empreinte MD5 locale
* l'ETag distant
* le statut de vérification

<br>

## Script d'ingestion

Le script `scripts/upload_to_minio.py` regroupe les quatre opérations :

1. création des buckets
2. application des politiques d'accès bucket
3. upload sans partitionnement
4. vérification d'intégrité et génération du manifeste