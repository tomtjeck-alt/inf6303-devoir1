-- DEVOIR 1 - Partie A : Hive/HiveQL
-- Complétez les zones TODO. Ne modifiez pas les chemins imposés.

CREATE DATABASE IF NOT EXISTS inf6303_devoir1;
USE inf6303_devoir1;
-- Un split par fichier : CombineHiveInputFormat (défaut) fusionne les 8 fichiers
-- en un seul split et n'ignore alors qu'un seul en-tête (1 000 008 lignes au lieu de 1 000 000).
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
-- Jointures en shuffle (common join) : la tâche locale de map join échoue dans ce conteneur.
SET hive.auto.convert.join=false;
-- Pas de calcul automatique des statistiques de colonnes après CREATE TABLE AS SELECT
-- (cette étape supplémentaire échoue avec le moteur MapReduce de Hive 4.0.0).
SET hive.stats.column.autogather=false;
-- TODO A1 : table externe evenements_massifs (10 colonnes)
-- LOCATION '/inf6303/devoir1/evenements'
DROP TABLE IF EXISTS evenements_massifs;
CREATE EXTERNAL TABLE evenements_massifs (
  event_id       STRING,
  user_id        STRING,
  content_id     STRING,
  event_type     STRING,
  event_ts       TIMESTAMP,
  country        STRING,
  device         STRING,
  watch_seconds  INT,
  quality        STRING,
  session_id     STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/inf6303/devoir1/evenements'
TBLPROPERTIES ("skip.header.line.count"="1");

-- TODO A2 : table externe catalogue_contenus (5 colonnes)
-- LOCATION '/inf6303/devoir1/catalogue'
DROP TABLE IF EXISTS catalogue_contenus;
CREATE EXTERNAL TABLE catalogue_contenus (
  content_id    STRING,
  title         STRING,
  genre         STRING,
  release_year  INT,
  duration_sec  INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/inf6303/devoir1/catalogue'
TBLPROPERTIES ("skip.header.line.count"="1");

-- TODO A3 : contrôles de qualité et analyses demandées dans l'énoncé.

-- Volume attendu : 1 000 000 événements et 2 000 contenus.
SELECT COUNT(*) AS nb_evenements FROM evenements_massifs;
SELECT COUNT(*) AS nb_contenus   FROM catalogue_contenus;

-- Identifiants invalides : NULL ou format inattendu.
-- Formats attendus : e + 9 chiffres, u + 6 chiffres, c + 5 chiffres, s + 7 chiffres.
SELECT
  SUM(CASE WHEN event_id   IS NULL OR NOT event_id   RLIKE '^e[0-9]{9}$' THEN 1 ELSE 0 END) AS event_id_invalides,
  SUM(CASE WHEN user_id    IS NULL OR NOT user_id    RLIKE '^u[0-9]{6}$' THEN 1 ELSE 0 END) AS user_id_invalides,
  SUM(CASE WHEN content_id IS NULL OR NOT content_id RLIKE '^c[0-9]{5}$' THEN 1 ELSE 0 END) AS content_id_invalides,
  SUM(CASE WHEN session_id IS NULL OR NOT session_id RLIKE '^s[0-9]{7}$' THEN 1 ELSE 0 END) AS session_id_invalides
FROM evenements_massifs;

-- Doublons d'event_id (doit être 0).
SELECT COUNT(*) - COUNT(DISTINCT event_id) AS event_id_doublons FROM evenements_massifs;

-- Durées et dates invalides : une valeur illisible devient NULL grâce au typage.
SELECT
  SUM(CASE WHEN watch_seconds IS NULL THEN 1 ELSE 0 END) AS duree_null,
  SUM(CASE WHEN watch_seconds < 0     THEN 1 ELSE 0 END) AS duree_negative,
  SUM(CASE WHEN watch_seconds = 0     THEN 1 ELSE 0 END) AS duree_zero,
  SUM(CASE WHEN event_ts IS NULL      THEN 1 ELSE 0 END) AS date_null,
  MIN(watch_seconds) AS duree_min,
  MAX(watch_seconds) AS duree_max,
  MIN(event_ts)      AS premiere_date,
  MAX(event_ts)      AS derniere_date
FROM evenements_massifs;

-- Intégrité référentielle : événements dont le contenu n'existe pas dans le catalogue.
SELECT COUNT(*) AS contenus_orphelins
FROM evenements_massifs e
LEFT JOIN catalogue_contenus c ON e.content_id = c.content_id
WHERE c.content_id IS NULL;

-- Cohérence métier : durée visionnée supérieure à la durée du contenu.
SELECT COUNT(*) AS duree_superieure_au_contenu
FROM evenements_massifs e
JOIN catalogue_contenus c ON e.content_id = c.content_id
WHERE e.watch_seconds > c.duration_sec;

-- A2. Répartition par pays.
SELECT country, COUNT(*) AS nb_evenements
FROM evenements_massifs
GROUP BY country
ORDER BY nb_evenements DESC;

-- A2. Répartition par type d'événement.
SELECT event_type, COUNT(*) AS nb_evenements
FROM evenements_massifs
GROUP BY event_type
ORDER BY nb_evenements DESC;

-- A2. Top 10 des contenus selon la durée totale visionnée (jointure avec le catalogue).
SELECT e.content_id, c.title, c.genre,
       SUM(e.watch_seconds) AS duree_totale_sec
FROM evenements_massifs e
JOIN catalogue_contenus c ON e.content_id = c.content_id
GROUP BY e.content_id, c.title, c.genre
ORDER BY duree_totale_sec DESC
LIMIT 10;

-- A2. Durée moyenne par appareil et qualité.
SELECT device, quality,
       COUNT(*)                     AS nb_evenements,
       ROUND(AVG(watch_seconds), 1) AS duree_moyenne_sec
FROM evenements_massifs
GROUP BY device, quality
ORDER BY device, quality;

-- A2. Plan d'exécution de la requête du top 10.
EXPLAIN
SELECT e.content_id, c.title, c.genre,
       SUM(e.watch_seconds) AS duree_totale_sec
FROM evenements_massifs e
JOIN catalogue_contenus c ON e.content_id = c.content_id
GROUP BY e.content_id, c.title, c.genre
ORDER BY duree_totale_sec DESC
LIMIT 10;

-- TODO A4 : produire puis exporter un segment par utilisateur.
-- Le répertoire HDFS imposé est /inf6303/devoir1/sorties/segments
-- Seuils (durée totale de visionnage par utilisateur) :
--   intensif    : total_watch_sec >= 55000
--   actif       : 30000 <= total_watch_sec < 55000
--   occasionnel : total_watch_sec < 30000
DROP TABLE IF EXISTS segments_utilisateurs;
CREATE TABLE segments_utilisateurs AS
SELECT
  user_id,
  MAX(country)        AS country,
  COUNT(*)            AS nb_events,
  SUM(watch_seconds)  AS total_watch_sec,
  CASE
    WHEN SUM(watch_seconds) >= 55000 THEN 'intensif'
    WHEN SUM(watch_seconds) >= 30000 THEN 'actif'
    ELSE 'occasionnel'
  END                 AS segment
FROM evenements_massifs
GROUP BY user_id;

-- Contrôle : un seul pays par utilisateur (doit être 0), sinon MAX(country) serait arbitraire.
SELECT COUNT(*) AS users_multi_pays
FROM (SELECT user_id FROM evenements_massifs GROUP BY user_id HAVING COUNT(DISTINCT country) > 1) t;

-- Contrôle : utilisateurs par segment (attendu : actif 30901, intensif 7430, occasionnel 11669).
SELECT segment, COUNT(*) AS nb_utilisateurs
FROM segments_utilisateurs
GROUP BY segment
ORDER BY segment;

-- Contrôle de conservation : la somme des nb_events doit redonner 1 000 000.
SELECT COUNT(*) AS nb_utilisateurs, SUM(nb_events) AS total_evenements
FROM segments_utilisateurs;

-- Export CSV vers le répertoire HDFS imposé.
-- Colonnes, dans l'ordre : user_id,country,nb_events,total_watch_sec,segment (sans en-tête).
INSERT OVERWRITE DIRECTORY '/inf6303/devoir1/sorties/segments'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT user_id, country, nb_events, total_watch_sec, segment
FROM segments_utilisateurs
ORDER BY user_id;
