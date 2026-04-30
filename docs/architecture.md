# Architecture

## Overview

This project simulates an end-to-end Data Engineering pipeline using batch ingestion, ETL processing, relational databases, event streaming, REST API exposure and distributed processing.

## Pipeline Flow

```text
CSV File
   ↓
stg_sales (MySQL - Staging Layer)
   ↓
raw_sales_clean (MySQL - Raw/Clean Layer)
   ↓
sales_clean_sqlserver (SQL Server - Serving Layer)
   ↓
Kafka Producer
   ↓
Kafka Topic: sales_clean_events
   ↓
Kafka Consumer
   ↓
kafka_sales_events (MySQL)
   ↓
Spring Boot API + Swagger
   ↓
Spark + Scala Processing
   ↓
Parquet Data Lake
````

## Layers

### STG Layer

The `stg_sales` table stores raw data loaded directly from the CSV file.
This layer represents the initial landing area before validation.

### RAW Layer

The `raw_sales_clean` table stores validated and transformed records.
Invalid records are redirected to the rejects table.

### Serving Layer

The `sales_clean_sqlserver` table exposes clean data in SQL Server, simulating a target operational or analytical system.

### Messaging Layer

Kafka is used to publish clean sales events and consume them into a dedicated event table.

### API Layer

The Spring Boot API exposes sales data through REST endpoints and Swagger documentation.

### Processing Layer

Spark and Scala read clean data from MySQL, apply partitioning and aggregations, and write analytical outputs in Parquet format.

## Main Concepts Demonstrated

* ETL pipeline design
* Staging and raw layers
* Data validation
* Reject handling
* ETL run logging
* Cross-database replication
* Kafka producer/consumer pattern
* REST API exposure
* Spark DataFrame processing
* Parquet-based Data Lake output

