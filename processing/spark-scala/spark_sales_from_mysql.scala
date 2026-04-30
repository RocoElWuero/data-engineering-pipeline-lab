/*
	Scala + Spark + Hadoop-compatible Data Lake demo
	https://github.com/steveloughran/winutils

	Objetivo:
	- Leer datos limpios desde MySQL usando Spark JDBC
	- Reparticionar los datos para demostrar partitioning
	- Aplicar transformaciones/agregaciones tipo ETL
	- Guardar resultados en formato Parquet, usando el FileSystem API de Hadoop
	- La ruta local file:///... puede cambiarse a hdfs://... si después montas HDFS real

	Ejecutar desde spark-shell:
	spark-shell --packages com.mysql:mysql-connector-j:8.4.0 -i spark_sales_from_mysql.scala
*/

/*
	C:\hadoop\bin\winutils.exe
	Es un ejecutable para que Spark pueda ejecutar operaciones del filesystem compatibles con Hadoop, ya que Hadoop está diseñado originalmente para entornos Unix:
		* Crear carpetas
		* Cambiar permisos (chmod)
		* Manejar archivos temporales
		* Validar accesos
	¿Por qué existe? Porque Hadoop fue hecho para Linux, no para Windows.
	Entonces en Windows "winutils.exe" actúa como "traductor" de esas operaciones.
*/

/*
	C:\hadoop\bin\hadoop.dll
	Es una librería nativa (C/C++) de Hadoop
	¿Para qué sirve?
		* Operaciones de bajo nivel
		* Manejo de archivos
		* Rendimiento optimizado
*/

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

def getEnvOrFail(key: String): String =
	sys.env.getOrElse(key, throw new RuntimeException(s"Missing env var: $key"))

val mysqlHost = sys.env.getOrElse("MYSQL_HOST", "127.0.0.1")
val mysqlPort = sys.env.getOrElse("MYSQL_PORT", "3306")
val mysqlDb   = sys.env.getOrElse("MYSQL_DATABASE", "data_engineering_pipeline_lab")

val mysqlUser = getEnvOrFail("MYSQL_USER")
val mysqlPassword = getEnvOrFail("MYSQL_PASSWORD")

val mysqlUrl = s"jdbc:mysql://$mysqlHost:$mysqlPort/$mysqlDb"

val outputRoot = sys.env.getOrElse("OUTPUT_ROOT", "file:///tmp/data_lake")
val cleanParquetPath = s"$outputRoot/raw_sales_clean_parquet"
val productKpiPath = s"$outputRoot/sales_kpi_by_product"
val regionKpiPath = s"$outputRoot/sales_kpi_by_region"

// Configuración Hadoop usada por Spark.
// Esto demuestra integración con Hadoop FileSystem aunque sea en modo local.
val hadoopConf = spark.sparkContext.hadoopConfiguration
hadoopConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

val fs = FileSystem.get(new URI(outputRoot), hadoopConf)

def deleteIfExists(pathText: String): Unit = {
	val path = new Path(pathText)
	if (fs.exists(path)) {
		println(s"Deleting previous output: $pathText")
		fs.delete(path, true)
	}
}

println("=======================================")
println(" Reading raw_sales_clean from MySQL")
println("=======================================")

val salesClean = spark.read
	.format("jdbc")
	.option("url", mysqlUrl)
	.option("dbtable", "raw_sales_clean")
	.option("user", mysqlUser)
	.option("password", mysqlPassword)
	.option("driver", "com.mysql.cj.jdbc.Driver")
	.load()

salesClean.printSchema()
salesClean.show(false)

println(s"Total clean records read from MySQL: ${salesClean.count()}")

println("=======================================")
println(" Spark partitioning demo")
println("=======================================")

val partitionedSales = salesClean
	.repartition(4, col("region_id"))
	.withColumn("processing_ts", current_timestamp())

println(s"Number of Spark partitions: ${partitionedSales.rdd.getNumPartitions}")

println("=======================================")
println(" Writing clean data to Hadoop-compatible Data Lake as Parquet")
println("=======================================")

deleteIfExists(cleanParquetPath)

partitionedSales.write
	.mode(SaveMode.Overwrite)
	.partitionBy("region_id")
	.parquet(cleanParquetPath)

println(s"Clean Parquet output written to: $cleanParquetPath")

println("=======================================")
println(" Building KPIs by product")
println("=======================================")

val salesByProduct = partitionedSales
	.groupBy("product_name")
	.agg(
		count("*").alias("sales_count"),
		sum("quantity").alias("total_quantity"),
		sum("total_amount").alias("total_revenue"),
		avg("unit_price").alias("avg_unit_price")
	)
	.orderBy(desc("total_revenue"))

salesByProduct.show(false)

deleteIfExists(productKpiPath)

salesByProduct.write
	.mode(SaveMode.Overwrite)
	.parquet(productKpiPath)

println(s"Product KPI Parquet output written to: $productKpiPath")

println("=======================================")
println(" Building KPIs by region")
println("=======================================")

val salesByRegion = partitionedSales
	.groupBy("region_id")
	.agg(
		count("*").alias("sales_count"),
		sum("quantity").alias("total_quantity"),
		sum("total_amount").alias("total_revenue")
	)
	.orderBy("region_id")

salesByRegion.show(false)

deleteIfExists(regionKpiPath)

salesByRegion.write
	.mode(SaveMode.Overwrite)
	.parquet(regionKpiPath)

println(s"Region KPI Parquet output written to: $regionKpiPath")

println("=======================================")
println(" Hadoop FileSystem listing")
println("=======================================")

val outputStatus = fs.listStatus(new Path(outputRoot))
outputStatus.foreach(status => println(status.getPath.toString))

println("=======================================")
println(" Spark + Scala + Hadoop demo finished successfully")
println("=======================================")
