from pyspark.sql import SparkSession

# Inicializar la sesión de Spark con el conector de MariaDB
spark = SparkSession.builder \
    .appName("Conexión MariaDB") \
    .config("spark.jars", "/home/hadoop/mariadb-java-client-2.7.3.jar") \
    .getOrCreate()

# Propiedades de conexión a MariaDB
jdbc_url = "jdbc:mysql://localhost/MovieBind"
connection_properties = {
    "user": "sqoop",
    "password": "pavilion+U",
    "driver": "org.mariadb.jdbc.Driver"
}

# Cargar la tabla USER en un DataFrame
user_dataframe = spark.read.jdbc(
    url=jdbc_url,
    table="Usuario",
    properties=connection_properties
)

# Cargar la tabla PROFILE en un DataFrame
profile_dataframe = spark.read.jdbc(
    url=jdbc_url,
    table="Perfil",
    properties=connection_properties
)

# Mostrar los esquemas de los DataFrames
print("Esquema de USER DataFrame:")
user_dataframe.printSchema()

print("Esquema de PROFILE DataFrame:")
profile_dataframe.printSchema()

# Realizar un join entre los dos DataFrames
# Nota: Ajusta la condición de join según las columnas comunes en tus tablas
movies_profile_dataframe = user_dataframe.join(
    profile_dataframe,
    user_dataframe.id_usuario == profile_dataframe.id_usuario,  # Ajusta según tus columnas
    "inner"
)

# Mostrar el DataFrame resultante
print("Esquema del DataFrame combinado:")
movies_profile_dataframe.printSchema()
movies_profile_dataframe.show(5)  # Mostrar primeras 5 filas