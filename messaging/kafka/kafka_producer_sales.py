from __future__ import annotations

import json
from datetime import date, datetime
#from datetime import date, datetime, UTC
from decimal import Decimal
from typing import Any

import os
from dotenv import load_dotenv
from pathlib import Path

import mysql.connector
from kafka import KafkaProducer
from mysql.connector import MySQLConnection

# 🔥 Cargar variables de entorno
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

def validate_env():
	required_vars = [
		"MYSQL_HOST", "MYSQL_PORT", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE",
		"SQLSERVER_HOST", "SQLSERVER_DATABASE", "SQLSERVER_USER", "SQLSERVER_PASSWORD"
	]

	missing = [var for var in required_vars if not os.getenv(var)]

	if missing:
		raise ValueError(f"Faltan variables de entorno: {missing}")

validate_env()

DB_CONFIG = {
	"host": os.getenv("MYSQL_HOST"),
	"port": int(os.getenv("MYSQL_PORT", "3306")),
	"user": os.getenv("MYSQL_USER"),
	"password": os.getenv("MYSQL_PASSWORD"),
	"database": os.getenv("MYSQL_DATABASE"),
}

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "sales_clean_events"


def get_connection() -> MySQLConnection:
	return mysql.connector.connect(**DB_CONFIG)


def json_serializer(value: dict[str, Any]) -> bytes:
	def default_serializer(obj: Any) -> str:
		if isinstance(obj, (datetime, date)):
			return obj.isoformat()
		if isinstance(obj, Decimal):
			return str(obj)
		return str(obj)

	return json.dumps(value, default=default_serializer).encode("utf-8")


def fetch_raw_sales_clean(conn: MySQLConnection) -> list[tuple]:
	query = """
		SELECT
			sale_id,
			sale_date,
			customer_name,
			product_name,
			region_id,
			quantity,
			unit_price,
			total_amount,
			ingestion_ts
		FROM raw_sales_clean
		ORDER BY sale_id
	"""
	with conn.cursor() as cursor:
		cursor.execute(query)
		return cursor.fetchall()


def main() -> None:
	conn = get_connection()
	rows = fetch_raw_sales_clean(conn)
	conn.close()

	producer = KafkaProducer(
		bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
		value_serializer=json_serializer,
		key_serializer=lambda k: str(k).encode("utf-8"),
	)

	sent = 0

	for row in rows:
		payload = {
			"source_sale_id": row[0],
			"sale_date": row[1],
			"customer_name": row[2],
			"product_name": row[3],
			"region_id": row[4],
			"quantity": row[5],
			"unit_price": row[6],
			"total_amount": row[7],
			"ingestion_ts": row[8],
			"event_type": "SALE_CLEAN_CREATED",
			"published_at": datetime.utcnow(), # DEPRECATED
			#"published_at": datetime.now(UTC),
		}

		future = producer.send(
			topic=KAFKA_TOPIC,
			key=row[0],
			value=payload,
		)
		metadata = future.get(timeout=10)
		sent += 1
		print(
			f"Sent sale_id={row[0]} to topic={metadata.topic} "
			f"partition={metadata.partition} offset={metadata.offset}"
		)

	producer.flush()
	producer.close()

	print(f"\nProducer finished successfully. Messages sent: {sent}")


if __name__ == "__main__":
	main()
