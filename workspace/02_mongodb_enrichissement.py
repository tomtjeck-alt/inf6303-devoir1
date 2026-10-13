"""DEVOIR 1 - Partie B. Compléter les TODO sans générer de nouvelles données."""
import csv, gzip, json
from pymongo import MongoClient, UpdateOne

client = MongoClient("mongodb://inf6303:inf6303@mongo:27017/?authSource=admin")
db = client["inf6303_devoir1"]
profiles = db["profils"]
DATA = "/home/jovyan/data"
OUT = "/home/jovyan/work/output"

# TODO B1 : vider la collection et importer profils_utilisateurs.jsonl.gz.
# drop() supprime aussi les anciens index : le script peut être relancé sans doublons.
profiles.drop()
with gzip.open(f"{DATA}/profils_utilisateurs.jsonl.gz", "rt", encoding="utf-8") as f:
    documents = [json.loads(ligne) for ligne in f if ligne.strip()]
# Chaque ligne est insérée telle quelle : le sous-document preferences
# et le tableau devices sont conservés sans transformation.
profiles.insert_many(documents)
print("B1. Profils importés :", profiles.count_documents({}))
print("B1. Exemple :", profiles.find_one({}, {"_id": 0}))

# TODO B2 : créer les index demandés.
# Index unique : un seul profil par user_id; il sert aussi aux mises à jour de B3.
profiles.create_index("user_id", unique=True, name="ux_user_id")
# Index composé : sert la requête d'audience « pays + segment »,
# la même que celle que Cassandra servira (partition country, segment).
profiles.create_index([("country", 1), ("analytics.segment", 1)], name="ix_country_segment")
print("B2. Index :", list(profiles.index_information().keys()))

# TODO B3 : lire output/segments_utilisateurs.csv et enrichir les profils
#           par user_id avec nb_events, total_watch_sec et segment.
# Le CSV produit par Hive n'a pas d'en-tête :
# user_id,country,nb_events,total_watch_sec,segment
operations = []
with open(f"{OUT}/segments_utilisateurs.csv", newline="", encoding="utf-8") as f:
    for user_id, country, nb_events, total_watch_sec, segment in csv.reader(f):
        operations.append(UpdateOne(
            {"user_id": user_id},
            {"$set": {"analytics": {
                "nb_events": int(nb_events),
                "total_watch_sec": int(total_watch_sec),
                "segment": segment,
                "source": "hive",
            }}},
        ))
resultat = profiles.bulk_write(operations, ordered=False)
print("B3. Lignes de segments lues :", len(operations))
print("B3. Profils trouvés :", resultat.matched_count, "| modifiés :", resultat.modified_count)

# TODO B4 : produire output/audience_cible.jsonl pour les segments intensif et actif.
filtre = {"consent_analytics": True, "analytics.segment": {"$in": ["intensif", "actif"]}}
projection = {"_id": 0, "user_id": 1, "country": 1, "plan": 1, "analytics": 1}
nb_exportes = 0
with open(f"{OUT}/audience_cible.jsonl", "w", encoding="utf-8") as f:
    for doc in profiles.find(filtre, projection).sort("user_id", 1):
        f.write(json.dumps(doc, ensure_ascii=False) + "\n")
        nb_exportes += 1
print("B4. Documents exportés :", nb_exportes)

# TODO B5 : afficher les contrôles demandés dans l'énoncé.
print("\n===== B5. CONTRÔLES =====")
print("Profils dans MongoDB      :", profiles.count_documents({}))
print("Segments (lignes Hive)    :", len(operations))
print("Profils enrichis          :", profiles.count_documents({"analytics": {"$exists": True}}))
print("Profils sans analytics    :", profiles.count_documents({"analytics": {"$exists": False}}))

print("\nProfils par segment :")
for r in profiles.aggregate([
    {"$group": {"_id": "$analytics.segment", "n": {"$sum": 1}}},
    {"$sort": {"_id": 1}},
]):
    print(f"  {r['_id']:<12} {r['n']}")

print("\nConsentement :")
for r in profiles.aggregate([
    {"$group": {"_id": "$consent_analytics", "n": {"$sum": 1}}},
    {"$sort": {"_id": 1}},
]):
    print(f"  {str(r['_id']):<12} {r['n']}")

print("\nAudience exportée par segment :")
for r in profiles.aggregate([
    {"$match": filtre},
    {"$group": {"_id": "$analytics.segment", "n": {"$sum": 1}}},
    {"$sort": {"_id": 1}},
]):
    print(f"  {r['_id']:<12} {r['n']}")
print("  total       ", nb_exportes)

# explain() d'une requête qui utilise l'index composé.
plan = profiles.find({"country": "Canada", "analytics.segment": "intensif"}).explain()
gagnant = plan["queryPlanner"]["winningPlan"]
gagnant = gagnant.get("queryPlan", gagnant)
while "inputStage" in gagnant and gagnant.get("stage") != "IXSCAN":
    gagnant = gagnant["inputStage"]
stats = plan["executionStats"]
print("\nexplain() : find({country: 'Canada', analytics.segment: 'intensif'})")
print("  étape             :", gagnant.get("stage"))
print("  index utilisé     :", gagnant.get("indexName"))
print("  clés examinées    :", stats["totalKeysExamined"])
print("  docs examinés     :", stats["totalDocsExamined"])
print("  docs retournés    :", stats["nReturned"])

