# Data Engineering Pipeline Lab

Proyecto práctico end-to-end de ingeniería de datos que simula un flujo real empresarial:

```text
CSV → STG → RAW → SQL Server → Kafka → API → Data Lake (Spark)
```

---

## 🚀 Arquitectura
CSV
↓
stg_sales (MySQL)
↓
raw_sales_clean (MySQL)
↓
sales_clean_sqlserver (SQL Server)
↓
Kafka (Producer / Consumer)
↓
Spring Boot API (REST + Swagger)
↓
Spark + Scala → Data Lake (Parquet)

---

## 🧠 Qué demuestra este proyecto
- Diseño de pipelines ETL reales
- Validación y limpieza de datos
- Integración entre múltiples tecnologías
- Orquestación de procesos
- Streaming con Kafka
- Exposición de datos vía API REST
- Procesamiento distribuido con Spark
- Data Lake con Parquet

---

## 🛠️ Tecnologías
- Python (ETL)
- MySQL (staging + raw)
- SQL Server (target)
- Kafka (event streaming)
- Java + Spring Boot (API REST)
- Scala + Spark (data processing)
- Bash / WSL (orquestación)

---

## 📂 Estructura del proyecto
etl/ → pipelines en Python
messaging/kafka/ → producer/consumer
api/ → Spring Boot REST API
processing/ → Spark + Scala
database/ → scripts DDL
data/ → archivos de entrada
scripts/ → orquestación

---

## ▶️ Ejecución del pipeline
```bash
bash scripts/run_full_pipeline_completed.sh
```

---

## 🔄 Flujo ETL
1. Carga CSV a `stg_sales`
2. Validación y limpieza
3. Inserción en `raw_sales_clean`
4. Replicación a SQL Server
5. Registro de errores en `etl_rejects`
6. Logging en `etl_run_log`

---

## 📡 Kafka
* Producer envía eventos desde `raw_sales_clean`
* Consumer persiste en `kafka_sales_events`

---

## ⚙️ API REST
### Endpoints:
* GET    /api/sales
* GET    /api/sales/{id}
* POST   /api/sales
* DELETE /api/sales/{id}

---

## 🔥 Spark + Data Lake
* Lectura desde MySQL
* Repartition por region_id
* KPIs:
	* ventas por producto
	* ventas por región
* Output en Parquet

---

# 📌 Autor
Oscar Rojas Castillo
Data Engineer | ETL Developer | SQL | Data Pipelines
