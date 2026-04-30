from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional, Tuple

import os
from dotenv import load_dotenv
from pathlib import Path

import mysql.connector
from mysql.connector import MySQLConnection
import pyodbc

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

# =========================
# MYSQL CONFIG (DINÁMICO)
# =========================
MYSQL_CONFIG = {
	"host": os.getenv("MYSQL_HOST"),
	"port": int(os.getenv("MYSQL_PORT", "3306")),
	"user": os.getenv("MYSQL_USER"),
	"password": os.getenv("MYSQL_PASSWORD"),
	"database": os.getenv("MYSQL_DATABASE"),
}

# =========================
# SQL SERVER CONFIG
# =========================
SQLSERVER_CONFIG = {
	"server": os.getenv("SQLSERVER_HOST"),
	"database": os.getenv("SQLSERVER_DATABASE"),
	"user": os.getenv("SQLSERVER_USER"),
	"password": os.getenv("SQLSERVER_PASSWORD"),
}


@dataclass
class SalesRecord:
	raw_id: int
	sale_date_raw: str
	customer_name_raw: Optional[str]
	product_raw: Optional[str]
	region_raw: Optional[str]
	quantity_raw: Optional[str]
	unit_price_raw: Optional[str]
	source_file: Optional[str]


def get_mysql_connection() -> MySQLConnection:
	return mysql.connector.connect(**MYSQL_CONFIG)


def get_sqlserver_connection() -> pyodbc.Connection:
	conn_str = (
		"DRIVER={ODBC Driver 18 for SQL Server};"
		f"SERVER={SQLSERVER_CONFIG['server']};"
		f"DATABASE={SQLSERVER_CONFIG['database']};"
		f"UID={SQLSERVER_CONFIG['user']};"
		f"PWD={SQLSERVER_CONFIG['password']};"
		"TrustServerCertificate=yes;"
	)
	return pyodbc.connect(conn_str)


def normalize_text(value: Optional[str]) -> str:
	if value is None:
		return ""
	return value.strip()


def parse_date(value: str) -> Optional[datetime.date]:
	try:
		return datetime.strptime(value.strip(), "%Y-%m-%d").date()
	except Exception:
		return None


def parse_positive_int(value: str) -> Optional[int]:
	try:
		parsed = int(value.strip())
		return parsed if parsed > 0 else None
	except Exception:
		return None


def parse_positive_decimal(value: str) -> Optional[Decimal]:
	try:
		parsed = Decimal(value.strip())
		return parsed if parsed > 0 else None
	except (InvalidOperation, AttributeError):
		return None


def get_region_map(conn: MySQLConnection) -> dict[str, int]:
	query = "SELECT region_id, region_name FROM dim_region"
	with conn.cursor() as cursor:
		cursor.execute(query)
		rows = cursor.fetchall()

	return {region_name.strip().upper(): region_id for region_id, region_name in rows}


def fetch_staging_rows(conn: MySQLConnection) -> list[SalesRecord]:
	query = """
		SELECT
			raw_id,
			sale_date_raw,
			customer_name_raw,
			product_raw,
			region_raw,
			quantity_raw,
			unit_price_raw,
			source_file
		FROM stg_sales
		ORDER BY raw_id
	"""
	with conn.cursor() as cursor:
		cursor.execute(query)
		rows = cursor.fetchall()

	return [SalesRecord(*row) for row in rows]


def validate_record(record: SalesRecord, region_map: dict[str, int]) -> Tuple[bool, str, Optional[dict]]:
	sale_date_str = normalize_text(record.sale_date_raw)
	customer_name = normalize_text(record.customer_name_raw)
	product_name = normalize_text(record.product_raw)
	region_name = normalize_text(record.region_raw).upper()
	quantity_str = normalize_text(record.quantity_raw)
	unit_price_str = normalize_text(record.unit_price_raw)

	sale_date = parse_date(sale_date_str)
	if sale_date is None:
		return False, "Invalid sale_date format. Expected YYYY-MM-DD.", None

	if not customer_name:
		return False, "Customer name is empty.", None

	if not product_name:
		return False, "Product name is empty.", None

	if region_name not in region_map:
		return False, f"Region '{region_name}' does not exist in dim_region.", None

	quantity = parse_positive_int(quantity_str)
	if quantity is None:
		return False, "Quantity must be a positive integer.", None

	unit_price = parse_positive_decimal(unit_price_str)
	if unit_price is None:
		return False, "Unit price must be a positive decimal.", None

	total_amount = Decimal(quantity) * unit_price

	clean_row = {
		"sale_id": record.raw_id,
		"sale_date": sale_date,
		"customer_name": customer_name,
		"product_name": product_name,
		"region_id": region_map[region_name],
		"quantity": quantity,
		"unit_price": unit_price,
		"total_amount": total_amount,
	}
	return True, "OK", clean_row


def insert_clean_row_mysql(conn: MySQLConnection, clean_row: dict) -> None:
	query = """
		INSERT INTO raw_sales_clean
		(
			sale_date,
			customer_name,
			product_name,
			region_id,
			quantity,
			unit_price,
			total_amount
		)
		VALUES
		(
			%(sale_date)s,
			%(customer_name)s,
			%(product_name)s,
			%(region_id)s,
			%(quantity)s,
			%(unit_price)s,
			%(total_amount)s
		)
	"""
	payload = {
		"sale_date": clean_row["sale_date"],
		"customer_name": clean_row["customer_name"],
		"product_name": clean_row["product_name"],
		"region_id": clean_row["region_id"],
		"quantity": clean_row["quantity"],
		"unit_price": clean_row["unit_price"],
		"total_amount": clean_row["total_amount"],
	}
	with conn.cursor() as cursor:
		cursor.execute(query, payload)


def insert_clean_row_sqlserver(conn: pyodbc.Connection, clean_row: dict) -> None:
	query = """
		INSERT INTO sales_clean_sqlserver
		(
			sale_id,
			sale_date,
			customer_name,
			product_name,
			region_id,
			quantity,
			unit_price,
			total_amount,
			ingestion_ts
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
	"""
	cursor = conn.cursor()
	cursor.execute(
		query,
		(
			clean_row["sale_id"],
			clean_row["sale_date"],
			clean_row["customer_name"],
			clean_row["product_name"],
			clean_row["region_id"],
			clean_row["quantity"],
			float(clean_row["unit_price"]),
			float(clean_row["total_amount"]),
		),
	)


def insert_reject_row(conn: MySQLConnection, record: SalesRecord, reject_reason: str) -> None:
	query = """
		INSERT INTO etl_rejects
		(
			sale_date_raw,
			customer_name_raw,
			product_raw,
			region_raw,
			quantity_raw,
			unit_price_raw,
			source_file,
			reject_reason
		)
		VALUES
		(
			%(sale_date_raw)s,
			%(customer_name_raw)s,
			%(product_raw)s,
			%(region_raw)s,
			%(quantity_raw)s,
			%(unit_price_raw)s,
			%(source_file)s,
			%(reject_reason)s
		)
	"""
	payload = {
		"sale_date_raw": record.sale_date_raw,
		"customer_name_raw": record.customer_name_raw,
		"product_raw": record.product_raw,
		"region_raw": record.region_raw,
		"quantity_raw": record.quantity_raw,
		"unit_price_raw": record.unit_price_raw,
		"source_file": record.source_file,
		"reject_reason": reject_reason,
	}
	with conn.cursor() as cursor:
		cursor.execute(query, payload)


def insert_run_log_start(conn: MySQLConnection, process_name: str) -> int:
	query = """
		INSERT INTO etl_run_log
		(
			process_name,
			records_read,
			records_loaded,
			records_rejected,
			status,
			message
		)
		VALUES
		(
			%s, 0, 0, 0, %s, %s
		)
	"""
	with conn.cursor() as cursor:
		cursor.execute(query, (process_name, "RUNNING", "ETL started"))
		run_id = cursor.lastrowid
	conn.commit()
	return run_id


def update_run_log_end(
	conn: MySQLConnection,
	run_id: int,
	records_read: int,
	records_loaded: int,
	records_rejected: int,
	status: str,
	message: str,
) -> None:
	query = """
		UPDATE etl_run_log
		SET
			records_read = %s,
			records_loaded = %s,
			records_rejected = %s,
			status = %s,
			message = %s,
			finished_at = CURRENT_TIMESTAMP
		WHERE run_id = %s
	"""
	with conn.cursor() as cursor:
		cursor.execute(
			query,
			(
				records_read,
				records_loaded,
				records_rejected,
				status,
				message,
				run_id,
			),
		)
	conn.commit()


def main() -> None:
	process_name = "etl_sales_pipeline_mysql_sqlserver"

	mysql_conn = get_mysql_connection()
	sqlserver_conn = get_sqlserver_connection()
	run_id = insert_run_log_start(mysql_conn, process_name)

	records_read = 0
	records_loaded = 0
	records_rejected = 0

	try:
		region_map = get_region_map(mysql_conn)
		staging_rows = fetch_staging_rows(mysql_conn)
		records_read = len(staging_rows)

		for record in staging_rows:
			is_valid, message, clean_row = validate_record(record, region_map)

			if is_valid and clean_row is not None:
				insert_clean_row_mysql(mysql_conn, clean_row)
				insert_clean_row_sqlserver(sqlserver_conn, clean_row)
				records_loaded += 1
			else:
				insert_reject_row(mysql_conn, record, message)
				records_rejected += 1

		mysql_conn.commit()
		sqlserver_conn.commit()

		update_run_log_end(
			conn=mysql_conn,
			run_id=run_id,
			records_read=records_read,
			records_loaded=records_loaded,
			records_rejected=records_rejected,
			status="SUCCESS",
			message="ETL completed successfully in MySQL and SQL Server",
		)

		print("ETL finished successfully")
		print(f"Records read	 : {records_read}")
		print(f"Records loaded   : {records_loaded}")
		print(f"Records rejected : {records_rejected}")

	except Exception as exc:
		mysql_conn.rollback()
		sqlserver_conn.rollback()

		update_run_log_end(
			conn=mysql_conn,
			run_id=run_id,
			records_read=records_read,
			records_loaded=records_loaded,
			records_rejected=records_rejected,
			status="FAILED",
			message=str(exc)[:500],
		)
		raise

	finally:
		sqlserver_conn.close()
		mysql_conn.close()


if __name__ == "__main__":
	main()
