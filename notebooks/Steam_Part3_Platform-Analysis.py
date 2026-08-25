# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Analyse de la plateforme Steam
# MAGIC ## Partie 3 - Analyse des plateformes
# MAGIC
# MAGIC Cette dernière partie analyse la disponibilité des jeux selon les systèmes d'exploitation
# MAGIC (Windows, Mac, Linux) et croise ces informations avec les genres.
# MAGIC
# MAGIC > **Environnement serverless** : visualisations natives Databricks (pas de PNG, pas de cache).
# MAGIC > Une fois les 3 notebooks exécutés et leurs visualisations ajoutées au dashboard,
# MAGIC > utiliser **Publish** sur chaque notebook pour générer les URLs publiques destinées au jury.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports et configuration

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Chargement et préparation des données

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
    F.col("platforms.windows").alias("on_windows"),
    F.col("platforms.mac").alias("on_mac"),
    F.col("platforms.linux").alias("on_linux"),
    F.col("genre"),
    F.col("positive"),
    F.col("negative"),
)

total_games = df.count()
print(f"Données chargées — {total_games:,} jeux")

# Dataframe explosé par genre (genre est une chaîne : split puis explode)
df_genres = (
    df
    .filter(F.col("genre").isNotNull())
    .select(
        "app_id", "name", "on_windows", "on_mac", "on_linux",
        F.explode(F.split(F.col("genre"), r",\s*")).alias("genre")
    )
    .withColumn("genre", F.trim(F.col("genre")))
    .filter(F.length(F.col("genre")) > 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Disponibilité des jeux par plateforme
# MAGIC
# MAGIC ### 3.1 Quelle proportion des jeux est disponible sur Windows, Mac et Linux ?
# MAGIC
# MAGIC > **Visualisation** : le résultat est en format large (une colonne par OS).
# MAGIC > On le repasse en format long ci-dessous pour un Bar chart propre.

# COMMAND ----------

platform_stats = df.agg(
    F.round(F.sum(F.col("on_windows").cast("int")) / total_games * 100, 2).alias("pct_windows"),
    F.round(F.sum(F.col("on_mac").cast("int")) / total_games * 100, 2).alias("pct_mac"),
    F.round(F.sum(F.col("on_linux").cast("int")) / total_games * 100, 2).alias("pct_linux"),
)

display(platform_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC Format long (une ligne par plateforme) pour la visualisation Bar.
# MAGIC
# MAGIC > **Visualisation** : **Bar**, X = `plateforme`, Y = `pourcentage`.

# COMMAND ----------

platform_long = df.select(
    F.round(F.avg(F.col("on_windows").cast("int")) * 100, 2).alias("Windows"),
    F.round(F.avg(F.col("on_mac").cast("int")) * 100, 2).alias("Mac"),
    F.round(F.avg(F.col("on_linux").cast("int")) * 100, 2).alias("Linux"),
)

# Passage large -> long via stack()
platform_long = platform_long.select(
    F.expr("stack(3, 'Windows', Windows, 'Mac', Mac, 'Linux', Linux) as (plateforme, pourcentage)")
)

display(platform_long)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Combien de jeux sont disponibles sur toutes les plateformes à la fois ?
# MAGIC
# MAGIC > **Visualisation** : **Pie**, labels = `label`, valeurs = `count`.

# COMMAND ----------

cross_platform = (
    df
    .withColumn("toutes_plateformes",
        (F.col("on_windows") == True) &
        (F.col("on_mac") == True) &
        (F.col("on_linux") == True)
    )
    .withColumn("label",
        F.when(F.col("toutes_plateformes"), "Windows + Mac + Linux")
         .otherwise("Pas multiplateforme complet")
    )
    .groupBy("label")
    .count()
)

display(cross_platform)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Certains genres sont-ils plus disponibles sur certaines plateformes ?
# MAGIC
# MAGIC On calcule, pour chacun des **12 genres les plus courants**, le pourcentage de jeux
# MAGIC disponibles sur Windows, Mac et Linux.
# MAGIC
# MAGIC > **Visualisation** : **Bar** groupé, X = `genre`, séries = `pct_windows`, `pct_mac`, `pct_linux`.

# COMMAND ----------

# Les 12 genres les plus courants
top_genres_list = [
    row.genre for row in (
        df_genres
        .groupBy("genre")
        .count()
        .orderBy(F.desc("count"))
        .limit(12)
        .select("genre")
        .collect()
    )
]

genre_platform = (
    df_genres
    .filter(F.col("genre").isin(top_genres_list))
    .groupBy("genre")
    .agg(
        F.round(F.avg(F.col("on_windows").cast("int")) * 100, 1).alias("pct_windows"),
        F.round(F.avg(F.col("on_mac").cast("int")) * 100, 1).alias("pct_mac"),
        F.round(F.avg(F.col("on_linux").cast("int")) * 100, 1).alias("pct_linux"),
        F.count("*").alias("nb_jeux"),
    )
    .orderBy(F.desc("pct_windows"))
)

display(genre_platform)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Synthèse globale du projet
# MAGIC
# MAGIC | Partie | Analyse | DataFrame à visualiser |
# MAGIC |--------|---------|------------------------|
# MAGIC | 1 | Top 15 éditeurs | `top_publishers` |
# MAGIC | 1 | Sorties par année | `games_per_year` |
# MAGIC | 1 | Distribution des prix | `price_dist` |
# MAGIC | 1 | Top 15 langues | `languages_df` |
# MAGIC | 1 | Restrictions d'âge | `age_ratings` |
# MAGIC | 2 | Top 20 genres | `genre_counts` |
# MAGIC | 2 | Ratio positif par genre | `genre_ratings` |
# MAGIC | 2 | Genres par éditeur | `publisher_genres` |
# MAGIC | 2 | Revenus estimés par genre | `genre_revenue` |
# MAGIC | 3 | Disponibilité par plateforme | `platform_long` |
# MAGIC | 3 | Genres par plateforme | `genre_platform` |
# MAGIC
# MAGIC ### Publication pour le jury
# MAGIC 1. Construire chaque visualisation native (`+` -> Visualization) sur les `display()`.
# MAGIC 2. Ajouter chacune au dashboard (**⋮ -> Add to Dashboard**).
# MAGIC 3. **Run all** sur les 3 notebooks et vérifier que toutes les sorties s'affichent.
# MAGIC 4. **Publish** chaque notebook -> récupérer les 3 URLs publiques à transmettre au jury.
