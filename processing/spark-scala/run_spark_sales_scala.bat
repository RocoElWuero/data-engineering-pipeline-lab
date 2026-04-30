@echo off
REM Ejecuta el demo Scala + Spark + Hadoop-compatible Data Lake

cd /d C:\Users\RocoElWuero\data-engineering-pipeline-lab

spark-shell --packages com.mysql:mysql-connector-j:8.4.0 -i spark_sales_from_mysql.scala

pause
