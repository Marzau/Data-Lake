from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count

# Initialiser Spark
spark = SparkSession.builder.appName("LogProcessing").getOrCreate()

# Étape 1 : Charger le fichier de logs
print("Chargement des données de logs...")
logs_df = spark.read.text("hdfs:///data/raw/logs/")

# Étape 2 : Nettoyage et extraction des données
print("Nettoyage et extraction des données...")
# Extraire les champs clés : timestamp, classe, niveau de log, message et ID
logs_cleaned = logs_df.withColumn("timestamp", regexp_extract("value", r'^(\S+ \S+)', 1)) \
                      .withColumn("class", regexp_extract("value", r'\] (\S+)', 1)) \
                      .withColumn("log_level", regexp_extract("value", r'\[(\w+)\]', 1)) \
                      .withColumn("message", regexp_extract("value", r'\] (.+) for id', 1)) \
                      .withColumn("id", regexp_extract("value", r'id (\d+)', 1))

# Supprimer les colonnes inutiles
logs_cleaned = logs_cleaned.drop("value")

# Étape 3 : Analyse des logs
print("Analyse des logs...")
# Compter le nombre total de logs
total_logs = logs_cleaned.count()

# Compter le nombre de logs par niveau
logs_by_level = logs_cleaned.groupBy("log_level").count().orderBy(col("count").desc())

# Trouver les logs contenant un ID manquant
missing_id_logs = logs_cleaned.filter(col("id").isNull())

# Étape 4 : Afficher les résultats
print(f"Nombre total de logs : {total_logs}")
print("Nombre de logs par niveau :")
logs_by_level.show()

print("Logs avec ID manquant :")
missing_id_logs.show()

# Étape 5 : Sauvegarder les données nettoyées
print("Sauvegarde des données nettoyées...")
logs_cleaned.write.mode("overwrite").csv("hdfs:///data/clean/logs/cleaned_logs.csv", header=True)
logs_cleaned.write.mode("overwrite").parquet("hdfs:///data/clean/logs/cleaned_logs.parquet")

# Arrêter Spark
spark.stop()