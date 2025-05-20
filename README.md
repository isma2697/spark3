# IAB_BIU_Tema_03_Practica_05_03_

## Descripción

Este proyecto corresponde a la práctica 5.3 del tema 3 del módulo IABD. Se trabaja con Apache Spark, Delta Lake, MariaDB y MongoDB aplicando los principios de la arquitectura Medallion.

---

## Contenido

### 1. `movies_profile_dataframe.py`
- Conexión a MariaDB.
- Recuperación de tablas `USER` y `PROFILE`.
- Realización de un join entre ambas y persistencia en DataFrame.

### 2. `non_relational_db.py`
- Diseño de base de datos NoSQL para un e-commerce utilizando MongoDB.
- Conexión desde Spark a MongoDB.
- Verificación de persistencia con un DataFrame.

### 3. `delta_lake_1.py`
- Persistencia de DataFrames en Delta Lake.
- Lectura y modificación usando SQL y la API de Delta Lake.
- Time travel con versión anterior.
- Carga de datos del fichero `bing_covid-19_data.parquet` en una tabla Delta.

### 4. `delta_lake_2.py`
Implementación completa de la Arquitectura Medallion:
- **Bronze:** limpieza, enriquecimiento y guardado de datos de ventas.
- **Silver:** procesamiento y partición de datos por año.
- **Gold:** análisis de ventas 2019 y ciudades top 10.
- **Auditoría:** inserción de ventas ocultas y actualización de capas.
- **Time travel:** recuperación y comparación de versiones.

---

## Comandos

En el archivo `comandos.txt` se encuentran los comandos necesarios para ejecutar los scripts Spark y realizar las instalaciones necesarias.

---

## Estructura esperada

.
├── comandos.txt
├── delta_lake_1.py
├── delta_lake_2.py
├── movies_profile_dataframe.py
├── non_relational_db.py
├── bing_covid-19_data.parquet
├── salesdata/
├── delta/
│ ├── bronze/
│ ├── silver/
│ └── gold/
└── Tema03_Practica_05_03.docx


---

## Requisitos

- Apache Spark
- MariaDB JDBC Connector
- MongoDB y `spark-mongodb-connector`
- delta-spark 3.1.0

---