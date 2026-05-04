from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

import os
from dotenv import load_dotenv
from pathlib import Path

import mysql.connector
from kafka import KafkaConsumer
from mysql.connector import MySQLConnection

# 🔥 Cargar variables de entorno
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

def validate_env():
	required_vars = [
		"MYSQL_HOST",
		"MYSQL_PORT",
		"MYSQL_USER",
		"MYSQL_PASSWORD",
		"MYSQL_DATABASE",
		"KAFKA_BOOTSTRAP_SERVERS",
		"KAFKA_TOPIC",
		"KAFKA_GROUP_ID",
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

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
	"KAFKA_BOOTSTRAP_SERVERS",
	"127.0.0.1:9092"
).split(",")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales_clean_events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sales_clean_consumer_group")


def get_connection() -> MySQLConnection:
	return mysql.connector.connect(**DB_CONFIG)


def upsert_kafka_event(conn: MySQLConnection, message: Any) -> None:
	payload = message.value

	query = """
		INSERT INTO kafka_sales_events
		(
			source_sale_id,
			sale_date,
			customer_name,
			product_name,
			region_id,
			quantity,
			unit_price,
			total_amount,
			topic_name,
			partition_id,
			offset_id,
			kafka_event_ts
		)
		VALUES
		(
			%(source_sale_id)s,
			%(sale_date)s,
			%(customer_name)s,
			%(product_name)s,
			%(region_id)s,
			%(quantity)s,
			%(unit_price)s,
			%(total_amount)s,
			%(topic_name)s,
			%(partition_id)s,
			%(offset_id)s,
			%(kafka_event_ts)s
		)
		ON DUPLICATE KEY UPDATE
			customer_name = VALUES(customer_name),
			product_name = VALUES(product_name),
			region_id = VALUES(region_id),
			quantity = VALUES(quantity),
			unit_price = VALUES(unit_price),
			total_amount = VALUES(total_amount),
			topic_name = VALUES(topic_name),
			partition_id = VALUES(partition_id),
			offset_id = VALUES(offset_id),
			kafka_event_ts = VALUES(kafka_event_ts)
	"""

	kafka_event_ts = payload.get("published_at")
	if kafka_event_ts:
		kafka_event_ts = datetime.fromisoformat(kafka_event_ts)
	else:
		kafka_event_ts = datetime.utcnow()

	db_payload = {
		"source_sale_id": int(payload["source_sale_id"]),
		"sale_date": payload["sale_date"],
		"customer_name": payload["customer_name"],
		"product_name": payload["product_name"],
		"region_id": int(payload["region_id"]),
		"quantity": int(payload["quantity"]),
		"unit_price": Decimal(str(payload["unit_price"])),
		"total_amount": Decimal(str(payload["total_amount"])),
		"topic_name": message.topic,
		"partition_id": int(message.partition),
		"offset_id": int(message.offset),
		"kafka_event_ts": kafka_event_ts,
	}

	with conn.cursor() as cursor:
		cursor.execute(query, db_payload)

def safe_json_deserializer(message: bytes):
	try:
		return json.loads(message.decode("utf-8"))
	except json.JSONDecodeError:
		return None

def main() -> None:
	print("Kafka bootstrap servers:", KAFKA_BOOTSTRAP_SERVERS)
	print("Kafka topic:", KAFKA_TOPIC)
	print("Kafka group id:", KAFKA_GROUP_ID)

	consumer = KafkaConsumer(
		KAFKA_TOPIC,
		bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
		group_id=KAFKA_GROUP_ID,
		auto_offset_reset="earliest", # ¿Desde dónde empiezo a leer los mensajes? earliest = Desde el offset 0; latest = El último
		enable_auto_commit=True,
		consumer_timeout_ms=5000,
		# value_deserializer=lambda m: json.loads(m.decode("utf-8")),
		value_deserializer=safe_json_deserializer,
		key_deserializer=lambda k: k.decode("utf-8") if k else None,
	)

	conn = get_connection()
	consumed = 0

	try:
		for message in consumer:
			if message.value is None:
				print(f"Skipping non-JSON message at offset={message.offset}")
				continue
			upsert_kafka_event(conn, message)
			conn.commit()
			consumed += 1
			print(
				f"Consumed sale_id={message.value['source_sale_id']} "
				f"from topic={message.topic} partition={message.partition} offset={message.offset}"
			)

		print(f"\nConsumer finished successfully. Messages consumed: {consumed}")

	finally:
		consumer.close()
		conn.close()


if __name__ == "__main__":
	main()
