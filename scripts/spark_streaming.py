from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when  # Import de 'when'

# Initialiser Spark
spark = SparkSession.builder.appName("CSVProcessing").getOrCreate()

# Lire les données depuis un fichier CSV sur HDFS
csv_path = "hdfs:///data/raw/streaming/Social_Network_Ads.csv"
data = spark.read.csv(csv_path, header=True, inferSchema=True)

# Afficher le schéma des données
print("Schéma des données :")
data.printSchema()

# Vérifier les colonnes disponibles
print("Colonnes disponibles :")
print(data.columns)

# Vérifier si les colonnes nécessaires existent
required_columns = {"Age", "EstimatedSalary", "Purchased"}
if not required_columns.issubset(set(data.columns)):
    raise ValueError(f"Le fichier CSV ne contient pas toutes les colonnes requises : {required_columns}")

# Transformation : Calculer le nombre d'achats et la moyenne des salaires par tranche d'âge
result = data.groupBy("Age").agg(
    count(when(col("Purchased") == 1, True)).alias("NumberOfPurchases"),  # Compter les achats
    avg("EstimatedSalary").alias("AverageSalary")  # Calculer le salaire moyen
)

# Afficher les résultats intermédiaires
print("Résultats par tranche d'âge :")
result.show()

# Sauvegarder les résultats sur HDFS
output_path = "hdfs:///data/clean/streaming/"
result.write.mode("overwrite").parquet(output_path)
print(f"Les résultats ont été sauvegardés dans {output_path}")

# Arrêter la session Spark
spark.stop()
