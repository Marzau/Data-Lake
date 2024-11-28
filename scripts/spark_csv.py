from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, when, regexp_replace, isnan, count

# Créer une session Spark
spark = SparkSession.builder.appName("CSVProcessing").getOrCreate()

# Charger le fichier CSV avec le délimiteur ;
data = spark.read.csv("hdfs:///data/raw/csv/", header=True, sep=";")

# Afficher le schéma initial
print("Schéma initial des données :")
data.printSchema()

# Étape 1 : Nettoyage des données
# - Supprimer les colonnes entièrement vides
# - Remplacer les valeurs nulles ou NaN par des valeurs par défaut

# Supprimer les colonnes entièrement vides
non_empty_cols = [c for c in data.columns if data.select(count(when(col(c).isNotNull(), c))).first()[0] > 0]
data = data.select(*non_empty_cols)

# Remplacer les valeurs nulles par des valeurs par défaut
data = data.fillna({
    "price": 0,
    "units_sold": 0,
    "rating": 0,
    "rating_count": 0,
    "product_color": "unknown",
    "product_variation_size_id": "unknown"
})

# Étape 2 : Transformation
# - Ajouter une colonne pour convertir les prix en devise standard (exemple : EUR en USD)
# - Ajouter une colonne pour vérifier si l'évaluation du produit est > 4.0

# Conversion simplifiée : Multiplier les prix en EUR par 1.1 pour obtenir une estimation en USD
data = data.withColumn("price_usd", col("price") * 1.1)

# Ajouter une colonne pour indiquer si le produit est bien noté
data = data.withColumn("high_rating", when(col("rating") > 4.0, 1).otherwise(0))

# Étape 3 : Analyse
# - Compter le nombre total de produits
# - Compter les produits bien notés
# - Trouver le produit le plus vendu

total_products = data.count()
high_rating_count = data.filter(col("high_rating") == 1).count()
most_sold_product = data.orderBy(col("units_sold").desc()).first()

# Afficher les résultats
print(f"Nombre total de produits : {total_products}")
print(f"Nombre de produits bien notés : {high_rating_count}")
print(f"Produit le plus vendu : {most_sold_product['title']} avec {most_sold_product['units_sold']} unités vendues")

# Étape 4 : Enregistrer les données nettoyées
# - Sauvegarder les données nettoyées dans un nouveau fichier CSV ou Parquet
data.write.mode("overwrite").csv("hdfs:///data/clean/csv/cleaned_csv.csv", header=True, sep=";")
data.write.mode("overwrite").parquet("hdfs:///data/clean/csv/cleaned_csv.parquet")

# Arrêter la session Spark
spark.stop()
