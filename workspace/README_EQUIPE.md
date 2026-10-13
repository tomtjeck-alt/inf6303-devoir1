# INF6303 - Devoir 1 - README d'équipe

Pipeline polyglotte : Hive sur HDFS -> MongoDB -> Cassandra.
Clé d'interconnexion entre les trois systèmes : `user_id`.

## Membres et répartition du travail

| Nom | Matricule | Contribution |
|---|---|---|
| Tjeck Tjeck Thomas Noel | tjen01@uqo.ca | Parties A, B, C et D, corrections de l'environnement, rapport, diagramme |

## Fichiers remis

| Fichier | Rôle |
|---|---|
| `01_hive_analyse.sql` | Partie A : tables externes, contrôles, analyses, segmentation, export HDFS |
| `02_mongodb_enrichissement.py` | Partie B : import des profils, index, enrichissement, export de l'audience |
| `03_cassandra_publication.py` | Partie C : tables orientées requête, chargement préparé, requêtes de service |
| `05_tracabilite.py` | Partie D1 : suivi de trois user_id de Hive à MongoDB puis Cassandra |
| `output/segments_utilisateurs.csv` | Fichier de passage Hive -> MongoDB (50 000 lignes) |
| `output/audience_cible.jsonl` | Fichier de passage MongoDB -> Cassandra (34 855 documents) |
| `output/resultats_*.txt` | Preuves d'exécution de chaque étape |

## Corrections apportées à l'environnement fourni

Sans ces corrections, les services ne démarraient pas ou les requêtes échouaient.

| Fichier | Correction | Raison |
|---|---|---|
| `docker/notebook.Dockerfile` | cassandra-driver 3.29.2 -> 3.29.3 | pas de wheel 3.29.2 pour Python 3.13 |
| `compose.yaml` (hive) | `user: "0:0"` | Derby ne pouvait pas créer /opt/hive/data/metastore_db |
| `compose.yaml` (notebook) | `JAVA_HOME: /usr/lib/jvm/java-21-openjdk-amd64` | « JAVA_HOME is not set » pour hdfs et beeline |
| `hadoop-config/yarn-site.xml` | scheduler = FifoScheduler | ResourceManager : « Queue configuration missing child queue names for root » |
| `hadoop-config/mapred-site.xml` | `HADOOP_MAPRED_HOME=/opt/hadoop` (AM, map, reduce) | MRAppMaster introuvable |
| `hive-config/hive-site.xml` | `hive.server2.enable.doAs=false` | « root is not allowed to impersonate jovyan » |
| `hive-config/hive-site.xml` | `hive.aux.jars.path` = commons-collections-3.2.2.jar | ClassNotFound CollectionUtils (absente de Hadoop 3.4.3) |
| `workspace/*.sh`, `*.sql` | fins de ligne CRLF -> LF (`sed -i 's/\r$//'`) | « invalid option name pipefail » |

Réglages ajoutés en tête de `01_hive_analyse.sql` :

- `hive.input.format=HiveInputFormat` : un split par fichier gzip; sinon un seul en-tête sur 8 est ignoré (1 000 008 lignes);
- `hive.auto.convert.join=false` : la tâche locale de map join échoue dans le conteneur;
- `hive.stats.column.autogather=false` : l'étape de statistiques après CREATE TABLE AS SELECT échoue (Hive 4.0.0 + MapReduce).

Après une modification de `hive-site.xml`, recréer le service (un simple restart échoue) :

```powershell
docker compose --profile hadoop up -d --force-recreate hive
```

## Procédure de reproduction

1. PowerShell, à la racine du projet :

```powershell
docker compose --profile hadoop up -d --build
```

2. Terminal JupyterLab (http://localhost:8888/lab?token=inf6303) :

```bash
bash /home/jovyan/work/00_deposer_dans_hdfs.sh
hdfs dfs -du -h /inf6303/devoir1/evenements
mkdir -p /home/jovyan/work/output
beeline -u jdbc:hive2://hive:10000/default -n jovyan --silent=true -f /home/jovyan/work/01_hive_analyse.sql | tee /home/jovyan/work/output/resultats_hive.txt
hdfs dfs -getmerge /inf6303/devoir1/sorties/segments /home/jovyan/work/output/segments_utilisateurs.csv
wc -l /home/jovyan/work/output/segments_utilisateurs.csv
```

3. PowerShell (arrêter Hadoop sans supprimer les volumes, puis démarrer MongoDB et Cassandra) :

```powershell
docker compose --profile hadoop stop
docker compose --profile nosql up -d --build
```

4. Terminal JupyterLab (Cassandra peut demander jusqu'à deux minutes avant d'accepter les connexions) :

```bash
python -u /home/jovyan/work/02_mongodb_enrichissement.py | tee /home/jovyan/work/output/resultats_mongodb.txt
python -u /home/jovyan/work/03_cassandra_publication.py | tee /home/jovyan/work/output/resultats_cassandra.txt
python -u /home/jovyan/work/05_tracabilite.py | tee /home/jovyan/work/output/resultats_tracabilite.txt
python /home/jovyan/work/04_validation_pipeline.py | tee /home/jovyan/work/output/resultats_validation.txt
```

Les scripts 02 et 03 sont idempotents (`drop()` / `DROP TABLE` puis rechargement) : ils peuvent être relancés
sans créer de doublons. Les fichiers de `output/` servent de points de reprise entre les systèmes.

## Résultats de contrôle attendus

| Étape | Valeur |
|---|---|
| HDFS : fichiers d'événements | 8 fichiers gzip, 23,4 Mo |
| Hive : événements / contenus | 1 000 000 / 2 000 |
| Hive : segments actif / intensif / occasionnel | 30 901 / 7 430 / 11 669 (= manifeste) |
| `segments_utilisateurs.csv` | 50 000 lignes |
| MongoDB : profils / profils enrichis | 50 000 / 50 000 |
| `audience_cible.jsonl` | 34 855 lignes (actif 28 071 + intensif 6 784) |
| Cassandra `audience_par_pays_segment` : lignes / écart | 34 855 / 0 |
| Cassandra `audience_par_plan` : lignes | 34 855 |

## Seuils de segmentation (total_watch_sec par utilisateur)

- intensif : >= 55 000
- actif : 30 000 à 54 999
- occasionnel : < 30 000

Le gabarit SQL ne contenait pas de seuils; ceux-ci ont été retenus parce qu'ils reproduisent exactement
les effectifs du manifeste (`data/MANIFESTE_DONNEES.json`).

## Remise à zéro complète

```powershell
docker compose --profile hadoop --profile nosql down -v
```