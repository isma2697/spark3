from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

# -----------------------------------------------------------
# 1. Crear la SparkSession con soporte para Delta Lake
# -----------------------------------------------------------
spark = SparkSession.builder \
    .appName("DeltaLakeExercise") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------------------------------------
# 2. Simular la persistencia de un DataFrame (ej. orders)
#    Este DataFrame simula los datos que obtuviste del apartado no relacional.
# -----------------------------------------------------------
data = [
    (1, "Alice", 99.99),
    (2, "Bob",   49.99)
]
columns = ["order_id", "customer", "total"]

df_orders = spark.createDataFrame(data, schema=columns)

# Ruta Delta para los datos de orders (ajusta la ruta según convenga)
delta_orders_path = "/tmp/delta/orders"

# Escribir el DataFrame en formato Delta (modo overwrite para asegurarse)
df_orders.write.format("delta").mode("overwrite").save(delta_orders_path)

# Registrar la tabla Delta mediante SQL
spark.sql(f"CREATE TABLE IF NOT EXISTS orders_delta USING DELTA LOCATION '{delta_orders_path}'")

# -----------------------------------------------------------
# 3. Leer la tabla Delta y mostrar los datos
# -----------------------------------------------------------
print("Datos originales (version 0):")
df = spark.read.format("delta").load(delta_orders_path)
df.show(truncate=False)

# -----------------------------------------------------------
# 4. Modificar los datos usando SQL
#    Por ejemplo, aumentar el total del pedido con order_id=1 en 10 unidades.
# -----------------------------------------------------------
spark.sql("UPDATE orders_delta SET total = total + 10 WHERE order_id = 1")

print("Después de actualizar (usando SQL):")
spark.sql("SELECT * FROM orders_delta").show(truncate=False)

# -----------------------------------------------------------
# 5. Realizar la misma modificación sin SQL usando la API de Delta Lake
#    Por ejemplo, aumentar el total del pedido con order_id=2 en 5 unidades.
# -----------------------------------------------------------
deltaTable = DeltaTable.forPath(spark, delta_orders_path)

deltaTable.update(
    condition = "order_id = 2",
    set = {"total": "total + 5"}
)

print("Después de la actualización (usando Delta API):")
spark.read.format("delta").load(delta_orders_path).show(truncate=False)

# -----------------------------------------------------------
# 6. Time Travel: Consultar una snapshot histórica.
#    Leemos la versión 0 (antes de cualquier actualización)
# -----------------------------------------------------------
print("Snapshot de la versión 0:")
df_version0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_orders_path)
df_version0.show(truncate=False)

# -----------------------------------------------------------
# 7. Cargar el fichero bing_covid-19_data.parquet y crear una tabla Delta a partir de él.
#    Ajusta la ruta al parquet según donde esté almacenado.
# -----------------------------------------------------------
parquet_path = "bing_covid-19_data.parquet"  # ruta al fichero Parquet
delta_covid_path = "/tmp/delta/covid"

# Leer el fichero Parquet
df_covid = spark.read.parquet(parquet_path)

# Persistir en Delta Lake (modo overwrite)
df_covid.write.format("delta").mode("overwrite").save(delta_covid_path)

# Registrar la tabla Delta
spark.sql(f"CREATE TABLE IF NOT EXISTS covid_data USING DELTA LOCATION '{delta_covid_path}'")

print("Datos del fichero bing_covid-19_data.parquet:")
spark.sql("SELECT * FROM covid_data").show(10, truncate=False)

# -----------------------------------------------------------
# Finalizar
# -----------------------------------------------------------
spark.stop()
