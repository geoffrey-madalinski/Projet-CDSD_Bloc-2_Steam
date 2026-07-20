# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Analyse de la plateforme Steam
# MAGIC ## Partie 2 — Analyse des genres
# MAGIC
# MAGIC On se concentre ici sur les genres de jeux : lesquels dominent le catalogue Steam,
# MAGIC lesquels ont les meilleures notes, quels éditeurs s'y spécialisent,
# MAGIC et lesquels semblent les plus rentables.
# MAGIC
# MAGIC > **Environnement serverless** : visualisations natives Databricks (pas de PNG, pas de cache).
# MAGIC > **Note sur les données** : `genre` est une **chaîne** (ex : `"Action, Indie"`), pas un tableau
# MAGIC > de structs. On la découpe donc avec `split()` avant `explode()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports et configuration

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Chargement et préparation des données
# MAGIC
# MAGIC On repart du JSON brut, on remonte les champs depuis `data`,
# MAGIC et on reconstruit le dataframe de base (avec la colonne `owners`).

# COMMAND ----------

raw_df = (
    spark.read
    .option("multiLine", "true")
    .json("s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json")
)

raw_df = raw_df.select("data.*")

df = raw_df.select(
    F.col("appid").alias("app_id"),
    F.col("name"),
    F.col("publisher"),
    F.col("price"),               # chaîne en dollars
    F.col("positive"),
    F.col("negative"),
    F.col("owners"),              # fourchette, ex : "20000 .. 50000"
    F.col("genre"),               # chaîne, ex : "Action, Indie"
)

# Prix : chaîne -> double, tolérant aux valeurs vides
df = df.withColumn("price", F.expr("try_cast(price AS double)"))

# is_free déduit du prix
df = df.withColumn("is_free", F.col("price") == 0)

# Ratio d'avis positifs
total_reviews = F.col("positive") + F.col("negative")
df = df.withColumn(
    "positive_ratio",
    F.when(total_reviews > 0, F.round(F.col("positive") / total_reviews, 4))
)

print(f"Données chargées — {df.count():,} jeux")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Création du dataframe par genre
# MAGIC
# MAGIC La colonne `genre` est une chaîne listant plusieurs genres séparés par des virgules.
# MAGIC On la découpe avec `split()` puis `explode()` pour créer une ligne par couple (jeu, genre) :
# MAGIC un jeu classé dans 3 genres donnera donc 3 lignes.

# COMMAND ----------

df_genres = (
    df
    .filter(F.col("genre").isNotNull())
    .select(
        "app_id", "name", "publisher", "price", "is_free",
        "positive", "negative", "positive_ratio", "owners",
        F.explode(F.split(F.col("genre"), r",\s*")).alias("genre")
    )
    .withColumn("genre", F.trim(F.col("genre")))
    .filter(F.length(F.col("genre")) > 0)
)

print(f"Dataframe genres — {df_genres.count():,} lignes (jeu × genre)")
display(df_genres.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Quels sont les genres les plus représentés ?
# MAGIC
# MAGIC > **Visualisation** : **Bar**, Y = `genre`, X = `count`.

# COMMAND ----------

genre_counts = (
    df_genres
    .groupBy("genre")
    .count()
    .orderBy(F.desc("count"))
    .limit(20)
)

display(genre_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Quels genres ont le meilleur ratio d'avis positifs ?
# MAGIC
# MAGIC Pour fiabiliser les résultats, on applique deux filtres :
# MAGIC - Minimum **50 avis** par jeu
# MAGIC - Minimum **50 jeux** par genre
# MAGIC
# MAGIC > **Visualisation** : **Bar**, Y = `genre`, X = `ratio_positif_moyen`.

# COMMAND ----------

genre_ratings = (
    df_genres
    .filter((F.col("positive") + F.col("negative")) >= 50)
    .groupBy("genre")
    .agg(
        F.count("*").alias("nb_jeux"),
        F.round(F.avg("positive_ratio"), 4).alias("ratio_positif_moyen"),
        F.sum("positive").alias("total_positif"),
        F.sum("negative").alias("total_negatif"),
    )
    .filter(F.col("nb_jeux") >= 50)
    .orderBy(F.desc("ratio_positif_moyen"))
)

display(genre_ratings)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Certains éditeurs ont-ils des genres de prédilection ?
# MAGIC
# MAGIC On regarde les **10 plus grands éditeurs** et leur répartition par genre.
# MAGIC
# MAGIC > **Visualisation** : **Bar** (empilé ou groupé), X = `publisher`, Y = `count`,
# MAGIC > série = `genre` — ou garder en Table pour lecture détaillée.

# COMMAND ----------

# Les 10 éditeurs avec le plus de jeux
top_10_publishers = (
    df
    .filter(F.col("publisher").isNotNull() & (F.trim(F.col("publisher")) != ""))
    .groupBy("publisher")
    .count()
    .orderBy(F.desc("count"))
    .limit(10)
    .select("publisher")
)

publisher_list = [row.publisher for row in top_10_publishers.collect()]

# Genres préférés de ces éditeurs
publisher_genres = (
    df_genres
    .filter(F.col("publisher").isin(publisher_list))
    .groupBy("publisher", "genre")
    .count()
    .orderBy("publisher", F.desc("count"))
)

display(publisher_genres)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Quels sont les genres les plus lucratifs ?
# MAGIC
# MAGIC On estime les revenus par genre en multipliant le prix moyen par la médiane
# MAGIC de la fourchette `owners` (ex : "20000 .. 50000" -> 35 000 copies).
# MAGIC C'est une estimation grossière mais elle donne un ordre de grandeur utile.
# MAGIC
# MAGIC > **Visualisation** : **Bar**, Y = `genre`, X = `revenus_estimes_M`.

# COMMAND ----------

df_revenue = (
    df_genres
    .filter(F.col("owners").isNotNull())
    .filter(F.col("price").isNotNull() & (F.col("price") > 0))
    # Extraction des deux nombres de la fourchette owners (quel que soit le séparateur)
    .withColumn("owners_min",
        F.expr(r"try_cast(regexp_extract(owners, '(\\d+)', 1) AS long)"))
    .withColumn("owners_max",
        F.expr(r"try_cast(regexp_extract(owners, '(\\d+)\\D+(\\d+)', 2) AS long)"))
    # Médiane de la fourchette comme estimation du nombre de copies
    .withColumn("owners_est", (F.col("owners_min") + F.col("owners_max")) / 2)
    .withColumn("revenue_est", F.col("price") * F.col("owners_est"))
)

genre_revenue = (
    df_revenue
    .filter(F.col("owners_est").isNotNull())
    .groupBy("genre")
    .agg(
        F.count("*").alias("nb_jeux"),
        F.round(F.avg("price"), 2).alias("prix_moyen"),
        F.round(F.avg("owners_est") / 1_000, 1).alias("proprietaires_moyens_k"),
        F.round(F.sum("revenue_est") / 1_000_000, 1).alias("revenus_estimes_M"),
    )
    .filter(F.col("nb_jeux") >= 20)
    .orderBy(F.desc("revenus_estimes_M"))
    .limit(15)
)

display(genre_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Résumé — Partie 2
# MAGIC
# MAGIC | Question | Visualisation à construire |
# MAGIC |---|---|
# MAGIC | Genre le plus représenté | Bar chart sur `genre_counts` |
# MAGIC | Genre le mieux noté | Bar chart sur `genre_ratings` |
# MAGIC | Genres par éditeur | Bar/Table sur `publisher_genres` |
# MAGIC | Genre le plus lucratif | Bar chart sur `genre_revenue` |
# MAGIC
# MAGIC **Suite** -> Partie 3 : Analyse des plateformes
