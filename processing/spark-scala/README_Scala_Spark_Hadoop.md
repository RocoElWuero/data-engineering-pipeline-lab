# Upgrade de la práctica: Scala + Spark + Hadoop-compatible Data Lake

## Qué agrega este módulo

Este módulo extiende tu práctica actual:

```text
CSV -> MySQL staging -> Python ETL -> MySQL clean + SQL Server clean
     -> Kafka producer/consumer
     -> Spring Boot API + Swagger
     -> Scala + Spark -> Data Lake Parquet
```

## Qué demuestra

- Scala como lenguaje para procesamiento de datos.
- Spark como motor de procesamiento distribuido.
- JDBC desde Spark para leer `raw_sales_clean` desde MySQL.
- Partitioning usando `repartition(4, col("region_id"))`.
- Transformaciones y agregaciones tipo ETL.
- Escritura en Parquet.
- Uso directo de Hadoop FileSystem API.
- Ruta local `file:///...` compatible con Hadoop; después puede cambiarse por `hdfs://...`.

## Qué NO es todavía

No levanta un clúster Hadoop real. Es una integración Hadoop-compatible usando Spark y Hadoop FileSystem en modo local.

Esto es intencional para evitar pelearse con instalación de HDFS en Windows antes de una entrevista. Conceptualmente, lo importante queda cubierto.

## Requisitos

1. Java 17 o compatible.
2. Apache Spark instalado localmente.
3. MySQL encendido.
4. Tu tabla `raw_sales_clean` con datos.
5. Variable de entorno `SPARK_HOME` configurada.
6. `spark-shell` disponible en PATH.

## Comando de ejecución

Desde la carpeta del proyecto:

```bat
spark-shell --packages com.mysql:mysql-connector-j:8.4.0 -i spark_sales_from_mysql.scala
```

## Si Spark no encuentra Java

Valida:

```bat
java --version
echo %JAVA_HOME%
```

## Si Spark no encuentra MySQL

Valida que MySQL esté encendido:

```bat
net start MySQL96
```

## Salidas generadas

Se crea una carpeta:

```text
C:\Users\RocoElWuero\practica_data_engineering\data_lake
```

Con:

```text
raw_sales_clean_parquet
sales_kpi_by_product
sales_kpi_by_region
```

## Cómo explicarlo en entrevista

> Extendí mi pipeline ETL agregando una capa de procesamiento distribuido con Scala y Spark. Spark lee la tabla limpia desde MySQL vía JDBC, reparte los datos por región para demostrar partitioning, genera KPIs agregados y escribe la salida en formato Parquet dentro de una estructura tipo data lake compatible con Hadoop FileSystem. Aunque la ejecución es local, la lógica puede moverse a HDFS cambiando la ruta de salida a hdfs://.

## Conceptos que cubre

- Batch processing
- Spark SQL / DataFrame API
- Partitioning
- Hadoop FileSystem
- Parquet
- Data Lake
- KPIs analíticos
- Escalabilidad horizontal conceptual
