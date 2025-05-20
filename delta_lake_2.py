from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, current_timestamp, lit, split, to_timestamp, year, month
from pyspark.sql.functions import split, trim, col
from delta.tables import DeltaTable
import os
from pyspark.sql import SparkSession

# Spark session configurado con Delta Lake
spark = SparkSession.builder \
    .appName("DeltaLakeSalesPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
# Definir base path
base_path = "/home/hadoop/Escritorio/Practica 5.3"

# Definir rutas completas
bronze_path = os.path.join(base_path, "delta/bronze/sales")
silver_path = os.path.join(base_path, "delta/silver/sales")
gold_ventas2019_path = os.path.join(base_path, "delta/gold/sales/ventas2019")
gold_top10_path = os.path.join(base_path, "delta/gold/sales/top10ciudades")
data_path = "./salesdata/salesdata/*.csv"  # Ruta local

# Crear todas las rutas si no existen
for path in [bronze_path, silver_path, gold_ventas2019_path, gold_top10_path]:
    os.makedirs(path, exist_ok=True)

# BRONZE
df_bronze = spark.read.option("header", "true").csv(data_path)

# Limpieza columnas
df_bronze = df_bronze.toDF(*[c.strip().replace(" ", "_") for c in df_bronze.columns])

# Añadir columnas ingestion_time y source_system
df_bronze = df_bronze.withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_system", lit("carga inicial CSV"))

df_bronze.write.format("delta").mode("overwrite").save(bronze_path)

# SILVER
df_silver = spark.read.format("delta").load(bronze_path)

# Eliminar filas con nulos
df_silver = df_silver.dropna()

# Eliminar filas con encabezados duplicados
df_silver = df_silver.filter(col("Product") != "Product")

# Extraer ciudad y estado
df_silver = df_silver.withColumn("City", trim(split(col("Purchase_Address"), ",")[1])) \
    .withColumn("State", split(split(col("Purchase_Address"), ",")[2], " ")[1]) \


# Convertir columna Order_Date a timestamp
df_silver = df_silver.withColumn("Order_Date", to_timestamp(col("Order_Date"), "MM/dd/yy HH:mm"))

# Crear columnas Month y Year
df_silver = df_silver.withColumn("Month", month("Order_Date")) \
    .withColumn("Year", year("Order_Date"))

df_silver.write.format("delta").mode("overwrite").partitionBy("Year").save(silver_path)

# GOLD - Ventas mensuales 2019
df_gold = spark.read.format("delta").load(silver_path)
df_gold = df_gold \
    .withColumn("Quantity_Ordered", col("Quantity_Ordered").cast(IntegerType())) \
    .withColumn("Price_Each", col("Price_Each").cast(FloatType())) \
    .withColumn("Sales", col("Quantity_Ordered") * col("Price_Each"))

ventas2019 = df_gold.filter(col("Year") == 2019) \
    .groupBy("Month") \
    .sum("Sales") \
    .withColumnRenamed("sum(Sales)", "Total_Sales")

ventas2019.write.format("delta").mode("overwrite").save(gold_ventas2019_path)
ventas2019.write.format("delta").mode("overwrite").saveAsTable("ventas2019_delta")

# GOLD - Top 10 ciudades
top10 = df_gold.groupBy("City") \
    .sum("Quantity_Ordered") \
    .withColumnRenamed("sum(Quantity_Ordered)", "Total_Quantity") \
    .orderBy(col("Total_Quantity").desc()) \
    .limit(10)

top10.write.format("delta").mode("overwrite").save(gold_top10_path)
top10.write.format("delta").mode("overwrite").saveAsTable("top10ciudades_delta")

# SIMULAR NUEVAS VENTAS
fake_data = [
    ("USB-C Cable", "136 Main St, Boston, MA 02108", "04/15/2019 10:00", 3, 33.0),
    ("iPhone", "50 Apple Way, Cupertino, CA 95014", "05/20/2019 15:45", 1, 700.0)
]

columns = ["Product", "Address", "Order_Date", "Quantity_Ordered", "Sales"]
df_fake = spark.createDataFrame(fake_data, columns)

# df_fake = df_fake.withColumnRenamed("Address", "Purchase_Address") \
#     .withColumn("Order_Date", col("Order_Date").cast(StringType())) \
#     .withColumn("City", trim(split(col("Purchase_Address"), ",")[1])) \
#     .withColumn("State", split(split(col("Purchase_Address"), ",")[2], " ")[1]) \
#     .withColumn("Quantity_Ordered", col("Quantity_Ordered").cast(StringType())) \
#     .withColumn("Price_Each", col("Sales").cast(StringType())) \
#     .withColumn("Month", month(to_timestamp(col("Order_Date"), "MM/dd/yy HH:mm"))) \
#     .withColumn("Year", year(to_timestamp(col("Order_Date"), "MM/dd/yy HH:mm"))) \
#     .withColumn("ingestion_time", current_timestamp()) \
#     .withColumn("source_system", lit("ajuste auditoría")) \
#     .drop("Sales")

df_fake = df_fake \
    .withColumnRenamed("Address", "Purchase_Address") \
    .withColumn("Order_ID", lit(None).cast(StringType())) \
    .withColumn("Order_Date", col("Order_Date").cast(StringType())) \
    .withColumn("Quantity_Ordered", col("Quantity_Ordered").cast(StringType())) \
    .withColumn("Price_Each", col("Sales").cast(StringType())) \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_system", lit("ajuste auditoría")) \
    .drop("Sales")    

# Guardar en Bronze
df_fake.write.format("delta").mode("append").save(bronze_path)

# Leer desde Bronze nuevamente
df_silver_updated = spark.read.format("delta").load(bronze_path).dropna() \
    .filter(col("Product") != "Product") \
    .withColumn("City", trim(split(col("Purchase_Address"), ",")[1])) \
    .withColumn("State", split(split(col("Purchase_Address"), ",")[2], " ")[1]) \
    .withColumn("Order_Date", to_timestamp(col("Order_Date"), "MM/dd/yy HH:mm")) \
    .withColumn("Month", month("Order_Date")) \
    .withColumn("Year", year("Order_Date")) \
    .withColumn("Quantity_Ordered", col("Quantity_Ordered").cast(IntegerType())) \
    .withColumn("Price_Each", col("Price_Each").cast(FloatType())) \
    .withColumn("Sales", col("Quantity_Ordered") * col("Price_Each"))

df_silver_updated.write.option("mergeSchema", "true").format("delta").mode("overwrite").partitionBy("Year").save(silver_path)


# Actualizar GOLD
ventas2019_updated = df_silver_updated.filter(col("Year") == 2019) \
    .groupBy("Month") \
    .sum("Sales") \
    .withColumnRenamed("sum(Sales)", "Total_Sales")

ventas2019_updated.write.format("delta").mode("overwrite").save(gold_ventas2019_path)

# TIME TRAVEL: mostrar versiones disponibles y comparar
dt = DeltaTable.forPath(spark, gold_ventas2019_path)
history_df = dt.history()
print("=== HISTORIAL DE LA TABLA GOLD VENTAS2019 ===")
history_df.show(truncate=False)

# Cargar versión inicial y actual
v0 = spark.read.format("delta").option("versionAsOf", 0).load(gold_ventas2019_path)
v0.show()

v_latest = spark.read.format("delta").load(gold_ventas2019_path)
v_latest.show()

spark.stop()
