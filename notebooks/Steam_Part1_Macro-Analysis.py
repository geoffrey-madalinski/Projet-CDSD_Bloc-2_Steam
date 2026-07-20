# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Analyse de la plateforme Steam
# MAGIC ## Partie 1 — Vue d'ensemble du marché (Analyse Macro)
# MAGIC
# MAGIC **Contexte** : Ubisoft souhaite mieux comprendre l'écosystème Steam avant de lancer un nouveau jeu.
# MAGIC Cette première partie couvre l'analyse macro : éditeurs les plus actifs, jeux les mieux notés,
# MAGIC évolution des sorties par année, distribution des prix, langues disponibles et restrictions d'âge.
# MAGIC
# MAGIC **Source de données** : `s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json`
# MAGIC
# MAGIC > **Environnement serverless** : DBFS FileStore est désactivé et le cache Spark non supporté.
# MAGIC > On ne sauvegarde donc pas de PNG : chaque tableau affiché par `display()` est converti en
# MAGIC > **visualisation native Databricks** (bouton `+` -> Visualization), puis ajouté au dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports et configuration

# COMMAND ----------

from pyspark.sql import functions as F

# En serverless : pas d'écriture de PNG (DBFS FileStore désactivé),
# pas de .cache() (PERSIST non supporté). Visualisations 100% natives Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Chargement du jeu de données depuis S3
# MAGIC
# MAGIC Le fichier JSON regroupe chaque jeu sous une structure `data` (avec un champ `id` en clé).
# MAGIC On remonte donc le contenu de `data` à la racine avec `select("data.*")`.

# COMMAND ----------

raw_df = (
    spark.read
    .option("multiLine", "true")
    .json("s3://full-stack-bigdata-datasets/Big_Data/Project_Steam/steam_game_output.json")
)

# Les champs utiles sont imbriqués sous "data" : on les remonte à la racine
raw_df = raw_df.select("data.*")

print(f"Nombre total de jeux chargés : {raw_df.count():,}")
print(f"Nombre de colonnes            : {len(raw_df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploration du schéma brut
# MAGIC
# MAGIC On affiche le schéma pour vérifier les champs disponibles avant extraction.

# COMMAND ----------

raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Préparation du dataframe principal
# MAGIC
# MAGIC Points d'attention sur ce jeu de données :
# MAGIC - `publisher` et `developer` sont des **chaînes simples** (pas des tableaux).
# MAGIC - `price` est une **chaîne en dollars** (ex : `"19.99"`), pas des centimes.
# MAGIC - `genre` est une **chaîne** (ex : `"Action, Indie"`), traitée en Partie 2.
# MAGIC - Plusieurs colonnes contiennent des chaînes vides `''` -> on utilise `try_cast`
# MAGIC   qui renvoie `null` au lieu de planter sur une valeur malformée.

# COMMAND ----------

df = raw_df.select(
    F.col("appid").alias("app_id"),
    F.col("name"),
    F.col("developer"),
    F.col("publisher"),
    F.col("release_date").alias("release_date_raw"),
    F.col("price"),               # chaîne en dollars, ex : "19.99"
    F.col("discount"),            # chaîne, ex : "0"
    F.col("positive"),
    F.col("negative"),
    F.col("owners"),
    F.col("required_age"),        # chaîne, ex : "0" ou "18"
    F.col("languages"),           # chaîne, ex : "English, French, ..."
    F.col("genre"),               # chaîne, ex : "Action, Indie"
    F.col("platforms.windows").alias("on_windows"),
    F.col("platforms.mac").alias("on_mac"),
    F.col("platforms.linux").alias("on_linux"),
)

# Prix : chaîne -> double (déjà en dollars). try_cast tolère les valeurs vides.
df = df.withColumn("price", F.expr("try_cast(price AS double)"))

# Pourcentage de réduction : chaîne -> entier, tolérant
df = df.withColumn("discount_pct", F.expr("try_cast(discount AS int)"))

# is_free n'existe pas dans les données : on le déduit du prix
df = df.withColumn("is_free", F.col("price") == 0)

# Année de sortie : extraction des 4 chiffres, puis cast tolérant
df = df.withColumn(
    "release_year",
    F.expr(r"try_cast(regexp_extract(release_date_raw, '(\\d{4})', 1) AS int)")
)

# Restriction d'âge : extraction du premier nombre, puis cast tolérant
df = df.withColumn(
    "required_age",
    F.expr(r"try_cast(regexp_extract(cast(required_age AS string), '(\\d+)', 1) AS int)")
)

# Ratio d'avis positifs (entre 0 et 1)
total_reviews = F.col("positive") + F.col("negative")
df = df.withColumn(
    "positive_ratio",
    F.when(total_reviews > 0, F.round(F.col("positive") / total_reviews, 4))
)

print(f"Dataframe prêt — {df.count():,} jeux, {len(df.columns)} colonnes")
display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Analyse macro
# MAGIC
# MAGIC ### 4.1 Quel éditeur a sorti le plus de jeux sur Steam ?
# MAGIC
# MAGIC > **Visualisation** : `+` -> Visualization -> **Bar**, Y = `publisher`, X = `count`.

# COMMAND ----------

top_publishers = (
    df
    .filter(F.col("publisher").isNotNull() & (F.trim(F.col("publisher")) != ""))
    .groupBy("publisher")
    .count()
    .orderBy(F.desc("count"))
    .limit(15)
)

display(top_publishers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Quels sont les jeux les mieux notés ?
# MAGIC
# MAGIC On filtre sur les jeux avec au moins **100 avis** pour que le ratio soit représentatif.
# MAGIC
# MAGIC > **Visualisation** : garder en **Table**, ou Bar avec Y = `name`, X = `positive_ratio`.

# COMMAND ----------

top_rated = (
    df
    .filter((F.col("positive") + F.col("negative")) >= 100)
    .filter(F.col("name").isNotNull())
    .orderBy(F.desc("positive_ratio"), F.desc("positive"))
    .select("name", "publisher", "positive", "negative", "positive_ratio")
    .limit(20)
)

display(top_rated)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Évolution des sorties par année
# MAGIC
# MAGIC Y a-t-il eu une variation notable pendant la pandémie de Covid (2020–2021) ?
# MAGIC
# MAGIC > **Visualisation** : **Bar**, X = `release_year`, Y = `count`.

# COMMAND ----------

games_per_year = (
    df
    .filter(F.col("release_year").isNotNull())
    .filter((F.col("release_year") >= 2000) & (F.col("release_year") <= 2023))
    .groupBy("release_year")
    .count()
    .orderBy("release_year")
)

display(games_per_year)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Comment les prix sont-ils distribués ?
# MAGIC
# MAGIC On sépare d'abord les jeux gratuits des jeux payants, puis on visualise la distribution des prix.
# MAGIC
# MAGIC > **Visualisation** : `free_vs_paid` en **Pie** (labels = `statut`, valeurs = `count`).

# COMMAND ----------

# Répartition gratuit / payant
free_vs_paid = (
    df
    .withColumn("statut", F.when(F.col("is_free") == True, "Gratuit").otherwise("Payant"))
    .groupBy("statut")
    .count()
)

display(free_vs_paid)

# COMMAND ----------

# MAGIC %md
# MAGIC Distribution des prix pour les jeux payants (on exclut les valeurs extrêmes au-delà de 60$).
# MAGIC
# MAGIC > **Visualisation** : **Histogram** sur la colonne `price` (~40 bins dans les options).

# COMMAND ----------

price_dist = (
    df
    .filter(F.col("price").isNotNull() & (F.col("price") > 0) & (F.col("price") <= 60))
    .select("price")
)

display(price_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Y a-t-il beaucoup de jeux avec une réduction ?
# MAGIC
# MAGIC > **Visualisation** : `discounts` en **Pie**, `discount_levels` en **Bar**.

# COMMAND ----------

# Répartition prix plein / en promo
discounts = (
    df
    .filter(F.col("discount_pct").isNotNull())
    .withColumn("en_promo",
        F.when(F.col("discount_pct") > 0, "En promotion").otherwise("Prix plein"))
    .groupBy("en_promo")
    .count()
)

display(discounts)

# COMMAND ----------

# Niveaux de réduction parmi les jeux en promo
discount_levels = (
    df
    .filter(F.col("discount_pct").isNotNull() & (F.col("discount_pct") > 0))
    .withColumn("niveau_promo",
        F.when(F.col("discount_pct") <= 25, "Jusqu'à -25%")
         .when(F.col("discount_pct") <= 50, "-26% à -50%")
         .when(F.col("discount_pct") <= 75, "-51% à -75%")
         .otherwise("-75% et plus")
    )
    .groupBy("niveau_promo")
    .count()
    .orderBy("niveau_promo")
)

display(discount_levels)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Quelles sont les langues les plus représentées ?
# MAGIC
# MAGIC La colonne `languages` est une chaîne (ex : `"English, French, German"`).
# MAGIC On utilise `split()` puis `explode()` pour compter chaque langue individuellement.
# MAGIC
# MAGIC > **Visualisation** : **Bar**, Y = `language`, X = `count`.

# COMMAND ----------

languages_df = (
    df
    .filter(F.col("languages").isNotNull())
    .select(
        F.explode(F.split(F.col("languages"), r",\s*")).alias("language")
    )
    .withColumn("language", F.trim(F.col("language")))
    .filter(F.length(F.col("language")) > 1)
    .groupBy("language")
    .count()
    .orderBy(F.desc("count"))
    .limit(15)
)

display(languages_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.7 Combien de jeux sont interdits aux mineurs ?
# MAGIC
# MAGIC > **Visualisation** : **Pie**, labels = `tranche_age`, valeurs = `count`.

# COMMAND ----------

age_ratings = (
    df
    .withColumn("tranche_age",
        F.when(F.col("required_age").isNull() | (F.col("required_age") == 0), "Tout public")
         .when(F.col("required_age") <= 12, "12+")
         .when(F.col("required_age") <= 16, "16+")
         .otherwise("18+")
    )
    .groupBy("tranche_age")
    .count()
    .orderBy("tranche_age")
)

display(age_ratings)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Résumé — Partie 1
# MAGIC
# MAGIC | Question | Visualisation à construire |
# MAGIC |---|---|
# MAGIC | Éditeur le plus actif | Bar chart sur `top_publishers` |
# MAGIC | Jeux les mieux notés | Table `top_rated` |
# MAGIC | Année record de sorties | Bar chart sur `games_per_year` |
# MAGIC | Prix médian | Histogram sur `price_dist` |
# MAGIC | Langue dominante | Bar chart sur `languages_df` (Anglais en tête) |
# MAGIC | Restriction 18+ | Pie chart sur `age_ratings` |
# MAGIC
# MAGIC **Suite** -> Partie 2 : Analyse des genres
