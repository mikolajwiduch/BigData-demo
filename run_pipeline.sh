#!/bin/bash
# Multi-Cloud Financial ETL Pipeline Runner
# This script orchestrates the Extraction and Transformation phases.

set -e # Exit immediately if a command exits with a non-zero status
set -o pipefail # Return value of a pipeline is the value of the last command to exit with a non-zero status

# Ensure required environment variables are set
REQUIRED_VARS=(
    "AWS_ACCESS_KEY_ID"
    "AWS_SECRET_ACCESS_KEY"
    "S3_BUCKET_NAME"
    "DB_HOST"
    "DB_USER"
    "DB_PASSWORD"
)

for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        echo "[ERROR] Mandatory environment variable $var is not set."
        exit 1
    fi
done

echo "======================================================"
echo "    Starting Financial ETL Pipeline Execution         "
echo "    Date: $(date "+%Y-%m-%d %H:%M:%S")                 "
echo "======================================================"

echo "[1/2] Running Extraction Phase (REST API -> AWS S3)..."
python src/extract.py
if [ $? -ne 0 ]; then
    echo "[ERROR] Extraction phase failed. Aborting pipeline."
    exit 1
fi
echo "[SUCCESS] Extraction phase completed."
echo "------------------------------------------------------"

echo "[2/2] Running Transformation & Load Phase (AWS S3 -> Pandas -> Azure DB)..."
python src/transform_load.py
if [ $? -ne 0 ]; then
    echo "[ERROR] Transformation and Load phase failed. Aborting pipeline."
    exit 1
fi
echo "[SUCCESS] Transformation and Load phase completed."

echo "======================================================"
echo "    Pipeline Execution Finished Successfully!         "
echo "======================================================"
