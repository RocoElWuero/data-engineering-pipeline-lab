-- PARA EL ETL (PYTHON) EN MySQL
DROP DATABASE IF EXISTS practica_data_engineering;
CREATE DATABASE practica_data_engineering;
USE practica_data_engineering;

CREATE TABLE dim_region (
	region_id INT PRIMARY KEY AUTO_INCREMENT,
	region_name VARCHAR(100) NOT NULL UNIQUE
);

INSERT INTO dim_region (region_name)
VALUES
('NORTE'),
('SUR'),
('CENTRO'),
('OCCIDENTE');

CREATE TABLE stg_sales (
	raw_id INT PRIMARY KEY AUTO_INCREMENT,
	sale_date_raw VARCHAR(50) NOT NULL,
	customer_name_raw VARCHAR(200),
	product_raw VARCHAR(200),
	region_raw VARCHAR(100),
	quantity_raw VARCHAR(50),
	unit_price_raw VARCHAR(50),
	source_file VARCHAR(255),
	ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE raw_sales_clean (
	sale_id INT PRIMARY KEY AUTO_INCREMENT,
	sale_date DATE NOT NULL,
	customer_name VARCHAR(200) NOT NULL,
	product_name VARCHAR(200) NOT NULL,
	region_id INT NOT NULL,
	quantity INT NOT NULL,
	unit_price DECIMAL(12,2) NOT NULL,
	total_amount DECIMAL(14,2) NOT NULL,
	ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_raw_sales_clean_region
		FOREIGN KEY (region_id) REFERENCES dim_region(region_id)
);

CREATE TABLE etl_rejects (
	reject_id INT PRIMARY KEY AUTO_INCREMENT,
	sale_date_raw VARCHAR(50),
	customer_name_raw VARCHAR(200),
	product_raw VARCHAR(200),
	region_raw VARCHAR(100),
	quantity_raw VARCHAR(50),
	unit_price_raw VARCHAR(50),
	source_file VARCHAR(255),
	reject_reason VARCHAR(500) NOT NULL,
	rejected_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE etl_run_log (
	run_id INT PRIMARY KEY AUTO_INCREMENT,
	process_name VARCHAR(100) NOT NULL,
	records_read INT NOT NULL DEFAULT 0,
	records_loaded INT NOT NULL DEFAULT 0,
	records_rejected INT NOT NULL DEFAULT 0,
	status VARCHAR(20) NOT NULL,
	message VARCHAR(500),
	started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	finished_at TIMESTAMP NULL
);

CREATE INDEX idx_stg_sales_region ON stg_sales(region_raw);
CREATE INDEX idx_raw_sales_clean_sale_date ON raw_sales_clean(sale_date);
CREATE INDEX idx_raw_sales_clean_region_id ON raw_sales_clean(region_id);

-- PARA KAFKA EN MySQL
USE practica_data_engineering;

CREATE TABLE IF NOT EXISTS kafka_sales_events (
	event_id BIGINT PRIMARY KEY AUTO_INCREMENT,
	source_sale_id INT NOT NULL UNIQUE,
	sale_date DATE NOT NULL,
	customer_name VARCHAR(200) NOT NULL,
	product_name VARCHAR(200) NOT NULL,
	region_id INT NOT NULL,
	quantity INT NOT NULL,
	unit_price DECIMAL(12,2) NOT NULL,
	total_amount DECIMAL(14,2) NOT NULL,
	topic_name VARCHAR(100) NOT NULL,
	partition_id INT NOT NULL,
	offset_id BIGINT NOT NULL,
	kafka_event_ts DATETIME NOT NULL,
	consumed_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- PARA SQL SERVER
CREATE DATABASE practica_sqlserver;
GO

USE practica_sqlserver;
GO

CREATE TABLE sales_clean_sqlserver (
	sale_id INT,
	sale_date DATE,
	customer_name VARCHAR(100),
	product_name VARCHAR(100),
	region_id INT,
	quantity INT,
	unit_price DECIMAL(10,2),
	total_amount DECIMAL(10,2),
	ingestion_ts DATETIME
);
GO
