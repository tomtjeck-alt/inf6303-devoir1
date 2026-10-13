"""DEVOIR 1 - Partie C. Publier l'audience produite par MongoDB."""
import json, time
from cassandra.cluster import Cluster
OUT = "/home/jovyan/work/output"
TABLE = "audience_par_pays_segment"

# TODO C1 : se connecter au service cassandra et créer le keyspace.
# Cassandra peut mettre une minute à accepter les connexions après son démarrage.
for tentative in range(1, 31):
    try:
        cluster = Cluster(["cassandra"], port=9042)
        session = cluster.connect()
        break
    except Exception as erreur:
        print(f"C1. Cassandra pas encore prête (tentative {tentative}) : {erreur}")
        time.sleep(5)
else:
    raise SystemExit("C1. Impossible de joindre Cassandra.")

# Un seul nœud dans ce laboratoire : SimpleStrategy avec un facteur de réplication de 1.
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS inf6303_devoir1
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace("inf6303_devoir1")
print("C1. Connecté, keyspace inf6303_devoir1 prêt.")

# TODO C2 : créer une table conçue pour la requête :
# top utilisateurs par (country, segment), score décroissant.
# Clé de partition composée (country, segment) : toutes les lignes d'un couple
# pays/segment sont stockées ensemble sur le même nœud.
# Clustering (score DESC, user_id) : les lignes sont déjà triées sur disque par
# score décroissant; user_id départage deux utilisateurs de même score
# (sinon le second écraserait le premier).
# score = total_watch_sec calculé par Hive.
session.execute(f"DROP TABLE IF EXISTS {TABLE}")
session.execute(f"""
    CREATE TABLE {TABLE} (
        country    text,
        segment    text,
        score      int,
        user_id    text,
        plan       text,
        nb_events  int,
        source     text,
        PRIMARY KEY ((country, segment), score, user_id)
    ) WITH CLUSTERING ORDER BY (score DESC, user_id ASC)
""")
print(f"C2. Table {TABLE} créée.")


# Deuxième table, pour le besoin « top utilisateurs d'un plan » :
# mêmes données, autre clé (dénormalisation : une table par requête).
# (plan, country) plutôt que (plan) seul : 3 plans donneraient 3 partitions chaudes.
session.execute("DROP TABLE IF EXISTS audience_par_plan")
session.execute("""
    CREATE TABLE audience_par_plan (
        plan       text,
        country    text,
        score      int,
        user_id    text,
        segment    text,
        nb_events  int,
        PRIMARY KEY ((plan, country), score, user_id)
    ) WITH CLUSTERING ORDER BY (score DESC, user_id ASC)
""")
print("C2. Table audience_par_plan créée.")

# TODO C3 : charger output/audience_cible.jsonl avec une requête préparée.
# La requête est analysée une seule fois par Cassandra, puis réutilisée
# pour chaque ligne avec des valeurs différentes (plus rapide, et pas d'injection).
insertion = session.prepare(f"""
    INSERT INTO {TABLE} (country, segment, score, user_id, plan, nb_events, source)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""")
insertion_plan = session.prepare("""
    INSERT INTO audience_par_plan (plan, country, score, user_id, segment, nb_events)
    VALUES (?, ?, ?, ?, ?, ?)
""")


nb_documents = 0
debut = time.time()
with open(f"{OUT}/audience_cible.jsonl", encoding="utf-8") as f:
    for ligne in f:
        doc = json.loads(ligne)
        a = doc["analytics"]
        session.execute(insertion, (
            doc["country"], a["segment"], a["total_watch_sec"],
            doc["user_id"], doc["plan"], a["nb_events"], "mongodb",
        ))
        session.execute(insertion_plan, (
         doc["plan"], doc["country"], a["total_watch_sec"],
         doc["user_id"], a["segment"], a["nb_events"],
        ))
        nb_documents += 1
print(f"C3. Documents lus dans audience_cible.jsonl : {nb_documents} ({time.time() - debut:.1f} s)")

# TODO C4 : exécuter les requêtes de validation demandées.


print("\n===== C4. VALIDATION =====")

# Comptage partition par partition : chaque requête vise une seule partition.
pays = sorted({r.country for r in session.execute(f"SELECT DISTINCT country, segment FROM {TABLE}")})
total_charge = 0
print("Lignes par partition (country, segment) :")
for p in pays:
    for s in ("intensif", "actif"):
        n = session.execute(
            f"SELECT COUNT(*) FROM {TABLE} WHERE country = %s AND segment = %s", (p, s)
        ).one()[0]
        total_charge += n
        print(f"  {p:<14} {s:<9} {n}")
print("Documents exportés par MongoDB :", nb_documents)
print("Lignes chargées dans Cassandra :", total_charge)
print("Écart                          :", nb_documents - total_charge)

def top(country, segment, limite=10):
    print(f"\nTop {limite} {country} / {segment} :")
    lignes = session.execute(
        f"SELECT user_id, score, nb_events, plan FROM {TABLE} "
        f"WHERE country = %s AND segment = %s LIMIT %s",
        (country, segment, limite),
    )
    for rang, r in enumerate(lignes, 1):
        print(f"  {rang:>2}. {r.user_id}  score={r.score:<6} nb_events={r.nb_events:<3} plan={r.plan}")

top("Canada", "intensif")
top("Senegal", "actif")

# Recherche globale par plan : plan n'est ni dans la clé de partition ni dans le clustering.
print("\nRecherche globale par plan : SELECT ... WHERE plan = 'premium'")
try:
    session.execute(f"SELECT user_id FROM {TABLE} WHERE plan = 'premium' LIMIT 5")
except Exception as erreur:
    print("  Refusée par Cassandra :", type(erreur).__name__)
    print("  ", str(erreur)[:200])

# Le même besoin, servi par la table audience_par_plan.
print("\nTop 5 premium / Canada (table audience_par_plan) :")
for rang, r in enumerate(session.execute(
    "SELECT user_id, score, segment FROM audience_par_plan "
    "WHERE plan = %s AND country = %s LIMIT 5", ("premium", "Canada")), 1):
    print(f"  {rang}. {r.user_id}  score={r.score:<6} segment={r.segment}")

# Contrôle : la deuxième table contient autant de lignes que la première.
total_plan = 0
print("\nLignes de audience_par_plan par plan :")
for plan in ("gratuit", "standard", "premium"):
    n = sum(session.execute(
        "SELECT COUNT(*) FROM audience_par_plan WHERE plan = %s AND country = %s", (plan, p)
    ).one()[0] for p in pays)
    total_plan += n
    print(f"  {plan:<9} {n}")
print("  total    ", total_plan, "(attendu :", nb_documents, ")")

cluster.shutdown()