import os
from dotenv import load_dotenv

import csv
import mysql.connector

from pathlib import Path

# 🔥 Cargar variables de entorno
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

def validate_env():
	required_vars = [
		"MYSQL_HOST", "MYSQL_PORT", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"
	]

	missing = [var for var in required_vars if not os.getenv(var)]

	if missing:
		raise ValueError(f"Faltan variables de entorno: {missing}")

validate_env()

BASE_DIR = Path(__file__).resolve().parents[2]
CSV_FILE = BASE_DIR / "data" / "input" / "sales_april.csv"

conn = mysql.connector.connect(
	host=os.getenv("MYSQL_HOST"),
	port=int(os.getenv("MYSQL_PORT", "3306")),
	user=os.getenv("MYSQL_USER"),
	password=os.getenv("MYSQL_PASSWORD"),
	database=os.getenv("MYSQL_DATABASE"),
)

cursor = conn.cursor()

"""
	Full load → TRUNCATE + INSERT
	Incremental → UPSERT / MERGE
	Streaming → append sin truncate
"""

print("Truncating staging table...")
cursor.execute("TRUNCATE TABLE stg_sales")
conn.commit()

sql = """
	INSERT INTO stg_sales
	(
		sale_date_raw,
		customer_name_raw,
		product_raw,
		region_raw,
		quantity_raw,
		unit_price_raw,
		source_file
	)
	VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

with open(CSV_FILE, mode="r", encoding="utf-8", newline="") as file:
	reader = csv.DictReader(file)
	rows = [
		(
			row["sale_date_raw"],
			row["customer_name_raw"],
			row["product_raw"],
			row["region_raw"],
			row["quantity_raw"],
			row["unit_price_raw"],
			row["source_file"],
		)
		for row in reader
	]

print("Filling staging table...")
cursor.executemany(sql, rows)
conn.commit()

print(f"Inserted rows: {cursor.rowcount}")

cursor.close()
conn.close()
