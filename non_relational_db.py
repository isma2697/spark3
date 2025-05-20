from pyspark.sql import SparkSession

# Crear SparkSession
spark = SparkSession.builder \
    .appName("MongoDBIntegrationFixed") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/ecommerce.orders") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Cargar datos como JSON crudo (para evitar inferencia vacía)
raw_df = spark.read.format("mongo").option("inferSchema", "false").load()

# Convertir a JSON string y luego volver a cargarlo como DataFrame clásico
json_rdd = raw_df.toJSON()
df = spark.read.json(json_rdd)
df.printSchema()
df.show(truncate=False)


# Mostrar esquema y contenido
df.printSchema()
df.show(truncate=False)


spark.stop()
