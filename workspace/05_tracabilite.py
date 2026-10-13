"""DEVOIR 1 - Partie D1. Suivre trois user_id de Hive à MongoDB puis à Cassandra."""
import csv, json
from pymongo import MongoClient
from cassandra.cluster import Cluster

OUT = "/home/jovyan/work/output"
profiles = MongoClient("mongodb://inf6303:inf6303@mongo:27017/?authSource=admin")["inf6303_devoir1"]["profils"]
session = Cluster(["cassandra"]).connect("inf6303_devoir1")

# Trois cas choisis dans MongoDB (aucun identifiant tapé à la main) :
#   1. intensif ou actif ET consentant   -> attendu dans les trois systèmes;
#   2. occasionnel                       -> absent de Cassandra (hors cible);
#   3. intensif ou actif NON consentant  -> absent de Cassandra (consentement).
cas = [
    ("cible consentante", {"analytics.segment": "intensif", "consent_analytics": True}),
    ("occasionnel", {"analytics.segment": "occasionnel"}),
    ("non consentant", {"analytics.segment": {"$in": ["intensif", "actif"]}, "consent_analytics": False}),
]
choix = [(libelle, profiles.find_one(filtre, sort=[("user_id", 1)])["user_id"]) for libelle, filtre in cas]

# Étape 1 : sortie Hive (CSV sans en-tête).
hive = {}
with open(f"{OUT}/segments_utilisateurs.csv", newline="", encoding="utf-8") as f:
    for user_id, country, nb_events, total_watch_sec, segment in csv.reader(f):
        hive[user_id] = (country, int(nb_events), int(total_watch_sec), segment)

# Étape 2 bis : fichier de passage MongoDB -> Cassandra.
exportes = set()
with open(f"{OUT}/audience_cible.jsonl", encoding="utf-8") as f:
    for ligne in f:
        exportes.add(json.loads(ligne)["user_id"])

# Lecture Cassandra par clé primaire complète (partition + clustering).
lecture = session.prepare(
    "SELECT user_id, score, nb_events, plan FROM audience_par_pays_segment "
    "WHERE country = ? AND segment = ? AND score = ? AND user_id = ?"
)

print(f"{'user_id':<9} {'cas':<18} {'système':<10} {'pays':<14} {'segment':<12} {'nb_events':>9} {'watch/score':>11}  remarque")
for libelle, uid in choix:
    country, nb_events, total, segment = hive[uid]
    print(f"{uid:<9} {libelle:<18} {'Hive':<10} {country:<14} {segment:<12} {nb_events:>9} {total:>11}")

    m = profiles.find_one({"user_id": uid})
    a = m["analytics"]
    print(f"{'':<9} {'':<18} {'MongoDB':<10} {m['country']:<14} {a['segment']:<12} {a['nb_events']:>9} {a['total_watch_sec']:>11}"
          f"  plan={m['plan']}, consent={m['consent_analytics']}, exporté={uid in exportes}")

    c = session.execute(lecture, (country, segment, total, uid)).one()
    if c:
        print(f"{'':<9} {'':<18} {'Cassandra':<10} {country:<14} {segment:<12} {c.nb_events:>9} {c.score:>11}  plan={c.plan}")
    else:
        raison = "segment hors cible" if segment == "occasionnel" else "pas de consentement"
        print(f"{'':<9} {'':<18} {'Cassandra':<10} {'absent':<14} {'':<12} {'':>9} {'':>11}  {raison} (filtre B3)")
    identique = (m["country"], a["nb_events"], a["total_watch_sec"], a["segment"]) == hive[uid]
    print(f"{'':<9} {'':<18} -> valeurs Hive = MongoDB : {identique}\n")