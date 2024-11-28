from pyspark.sql import SparkSession

# Initialiser Spark
spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

# Charger les données depuis HDFS
raw_data = spark.read.csv("hdfs:///data/raw/", header=True, inferSchema=True)

# Transformation (exemple : supprimer les valeurs nulles)
clean_data = raw_data.dropna()

# Sauvegarder les données nettoyées sur HDFS
clean_data.write.csv("hdfs:///data/clean/", header=True)
