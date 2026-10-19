#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Define Airflow home directory and version constraints
export AIRFLOW_HOME=~/airflow
AIRFLOW_VERSION="2.10.2" # Adjust to the latest stable version if needed
PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://githubusercontent.com{AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "=== Installing Python dependencies ==="
sudo apt update
sudo apt install -y python3-pip python3-venv build-essential libssl-dev libffi-dev

echo "=== Creating isolated Python Virtual Environment ==="
mkdir -p ~/airflow_env
python3 -m venv ~/airflow_env
source ~/airflow_env/bin/activate

echo "=== Upgrading pip ==="
pip install --upgrade pip

echo "=== Installing Apache Airflow ==="
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

echo "=== Initializing Airflow Database (SQLite default) ==="
airflow db init

echo "=== Creating default Admin User ==="
# Change these credentials for production use
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin_password

echo "=== Apache Airflow installation complete! ==="
echo "--------------------------------------------------------"
echo "To start your Airflow components, run the following commands:"
echo "1. Activate environment: source ~/airflow_env/bin/activate"
echo "2. Start Webserver:     airflow webserver --port 8080 -D"
echo "3. Start Scheduler:     airflow scheduler -D"
echo "--------------------------------------------------------"
