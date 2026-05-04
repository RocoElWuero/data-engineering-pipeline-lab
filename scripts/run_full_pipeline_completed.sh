#!/usr/bin/env bash

# ########################################################################## 
# Kafka "Demonizado" en Windows
# schtasks /Create /TN "Start Kafka KRaft" /TR "C:\kafka\start-kafka.bat" /SC ONLOGON /RL HIGHEST
# C:\kafka\bin\windows\kafka-server-start.bat config\kraft\server.properties
# ##########################################################################

# Termina inmediatamente con un error si intenta utilizar una variable que no ha sido definida previamente
set -u

# PROJECT_DIR_WSL="/mnt/c/Users/RocoElWuero/data-engineering-pipeline-lab"
# ROJECT_DIR_WIN="C:\\Users\\RocoElWuero\\data-engineering-pipeline-lab"

# Detecta automáticamente la ruta del proyecto
PROJECT_DIR_WSL="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR_WIN="$(wslpath -w "${PROJECT_DIR_WSL}")"

CSV_LOADER_WIN="${PROJECT_DIR_WIN}\\etl\\python\\load_csv_to_staging.py"
ETL_SCRIPT_WIN="${PROJECT_DIR_WIN}\\etl\\python\\etl_sales_pipeline_mysql_sqlserver.py"
KAFKA_PRODUCER_WIN="${PROJECT_DIR_WIN}\\messaging\\kafka\\kafka_producer_sales.py"
KAFKA_CONSUMER_WIN="${PROJECT_DIR_WIN}\\messaging\\kafka\\kafka_consumer_sales.py"

RUN_CSV_LOADER="true"
ETL_PIPELINE="true"
RUN_KAFKA_PRODUCER="true"
RUN_KAFKA_CONSUMER="true"

LOG_DIR_WSL="${PROJECT_DIR_WSL}/logs"
mkdir -p "${LOG_DIR_WSL}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR_WSL}/pipeline_${TIMESTAMP}.log"

NC='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'

log() {
	echo -e "$1" | tee -a "${LOG_FILE}"
}

info() {
	log "${CYAN}[INFO]${NC} $1"
}

ok() {
	log "${GREEN}[OK]${NC} $1"
}

warn() {
	log "${YELLOW}[WARN]${NC} $1"
}

fail() {
	log "${RED}[ERROR]${NC} $1"
	exit 1
}

run_cmd() {
	local description="$1"
	shift

	info "${description}"
	"$@" 2>&1 | tee -a "${LOG_FILE}"
	local status=${PIPESTATUS[0]}

	if [ "${status}" -ne 0 ]; then
		fail "${description} failed with exit code ${status}."
	fi

	ok "${description} completed."
}

command -v net.exe >/dev/null 2>&1 || fail "net.exe is not available from WSL."
command -v python.exe >/dev/null 2>&1 || fail "python.exe is not available from WSL."

if [ ! -f "${PROJECT_DIR_WSL}/etl/python/etl_sales_pipeline_mysql_sqlserver.py" ]; then
	fail "ETL script not found in ${PROJECT_DIR_WSL}."
fi

# Cargar variables desde .env si existe
ENV_FILE="${PROJECT_DIR_WSL}/.env"

if [ -f "${ENV_FILE}" ]; then
	set -a
	source "${ENV_FILE}"
	set +a

	info "Environment variables loaded from .env"
else
	warn ".env file not found at ${ENV_FILE}"
fi

log "${GREEN}========================================${NC}"
log "${GREEN} Starting Data Pipeline from WSL${NC}"
log "${GREEN} Log file: ${LOG_FILE}${NC}"
log "${GREEN}========================================${NC}"

run_cmd "Starting MySQL service..." #net.exe start MySQL96

info "Starting SQL Server Express service if available..."
#powershell.exe -NoProfile -Command "Start-Service -Name 'MSSQL\$SQLEXPRESS' -ErrorAction SilentlyContinue" 2>&1 | tee -a "${LOG_FILE}" >/dev/null
#powershell.exe -NoProfile -Command "Start-Service -DisplayName 'SQL Server (SQLEXPRESS)' -ErrorAction SilentlyContinue" 2>&1 | tee -a "${LOG_FILE}" >/dev/null
ok "SQL Server start command issued."

if [ "${RUN_CSV_LOADER}" = "true" ]; then
	if [ ! -f "${PROJECT_DIR_WSL}/etl/python/load_csv_to_staging.py" ]; then
		fail "load_csv_to_staging.py not found in ${PROJECT_DIR_WSL}."
	fi
	run_cmd "Loading CSV into staging" python.exe "${CSV_LOADER_WIN}"
else
	warn "Skipping CSV loader. Set RUN_CSV_LOADER=true to enable it."
fi

if [ "${ETL_PIPELINE}" = "true" ]; then
	if [ ! -f "${PROJECT_DIR_WSL}/etl/python/etl_sales_pipeline_mysql_sqlserver.py" ]; then
		fail "etl_sales_pipeline_mysql_sqlserver.py not found in ${PROJECT_DIR_WSL}."
	fi
	run_cmd "Running ETL pipeline (MySQL + SQL Server)" python.exe "${ETL_SCRIPT_WIN}"
else
	warn "Skipping ETL pipeline. Set ETL_PIPELINE=true to enable it."
fi

if [ "${RUN_KAFKA_PRODUCER}" = "true" ]; then
	if [ ! -f "${PROJECT_DIR_WSL}/messaging/kafka/kafka_producer_sales.py" ]; then
		fail "kafka_producer_sales.py not found in ${PROJECT_DIR_WSL}."
	fi
	run_cmd "Sending events to Kafka" python.exe "${KAFKA_PRODUCER_WIN}"
else
	warn "Skipping Kafka producer. Set RUN_KAFKA_PRODUCER=true to enable it."
fi

if [ "${RUN_KAFKA_CONSUMER}" = "true" ]; then
	if [ ! -f "${PROJECT_DIR_WSL}/messaging/kafka/kafka_consumer_sales.py" ]; then
		fail "kafka_consumer_sales.py not found in ${PROJECT_DIR_WSL}."
	fi
	run_cmd "Reading events to Kafka" python.exe "${KAFKA_CONSUMER_WIN}"
else
	warn "Skipping Kafka consumer. Set RUN_KAFKA_CONSUMER=true to enable it."
fi

log "${GREEN}========================================${NC}"
ok "Pipeline finished successfully."
info "Review detailed output in: ${LOG_FILE}"
log "${GREEN}========================================${NC}"

exit 0
