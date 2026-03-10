# Dockerfile for Multi-Cloud Financial ETL Pipeline

# Use a lightweight Debian-based Python image
FROM python:3.12-slim

# Install system dependencies required for psycopg2 compilation
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy dependency list and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the source code
COPY . .

# Ensure the orchestration script has execute permissions
RUN chmod +x run_pipeline.sh

# Main entrypoint runs the bash orchestrator
CMD ["./run_pipeline.sh"]
