from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, when

# Créer une session Spark
spark = SparkSession.builder.appName("JSONProcessing").getOrCreate()

# Charger le fichier JSON
data = spark.read.json("hdfs:///data/raw/json/")

# Afficher le schéma initial
print("Schéma initial des données :")
data.printSchema()

# Étape 1 : Nettoyage des données
# - Supprimer les lignes avec des valeurs nulles ou vides
# - Filtrer les enregistrements où "headline" est trop court (par exemple, moins de 5 caractères)

cleaned_data = (
    data.filter(col("article_link").isNotNull() & col("headline").isNotNull())
        .filter(length(col("headline")) > 5)
)

# Étape 2 : Transformation
# - Ajouter une colonne pour indiquer si le titre contient un mot-clé spécifique
# - Exemple : Vérifier si "political" apparaît dans le titre

transformed_data = cleaned_data.withColumn(
    "contains_political", when(col("headline").like("%political%"), 1).otherwise(0)
)

# Étape 3 : Analyse
# - Compter le nombre total de titres
# - Compter les titres sarcastiques et non sarcastiques
# - Compter les titres contenant le mot-clé "political"

total_count = transformed_data.count()
sarcastic_count = transformed_data.filter(col("is_sarcastic") == 1).count()
non_sarcastic_count = transformed_data.filter(col("is_sarcastic") == 0).count()
political_count = transformed_data.filter(col("contains_political") == 1).count()

# Afficher les résultats
print(f"Nombre total de titres : {total_count}")
print(f"Nombre de titres sarcastiques : {sarcastic_count}")
print(f"Nombre de titres non sarcastiques : {non_sarcastic_count}")
print(f"Nombre de titres contenant 'political' : {political_count}")

# Étape 4 : Enregistrer les données nettoyées
# - Sauvegarder les données transformées au format Parquet ou CSV
transformed_data.write.mode("overwrite").parquet("hdfs:///data/clean/json/cleaned_json.parquet")

# Arrêter la session Spark
spark.stop()