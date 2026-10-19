#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "=== Installing prerequisites ==="
sudo apt-get install -y apt-transport-https ca-certificates dirmngr

echo "=== Adding ClickHouse GPG Key ==="
sudo mkdir -p /usr/local/share/keyrings
curl -fsSL https://clickhouse.com | sudo gpg --dearmor -o /usr/local/share/keyrings/clickhouse-keyring.gpg

echo "=== Adding ClickHouse Repository ==="
echo "deb [signed-by=/usr/local/share/keyrings/clickhouse-keyring.gpg] https://clickhouse.com stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list

echo "=== Updating package list ==="
sudo apt-get update

echo "=== Installing ClickHouse Server and Client ==="
# Note: The installer may prompt you to set a password for the default user.
# If you run this in a purely automated CI/CD environment, prefix with: DEBIAN_FRONTEND=noninteractive
sudo apt-get install -y clickhouse-server clickhouse-client

echo "=== Starting ClickHouse Service ==="
sudo systemctl start clickhouse-server
sudo systemctl enable clickhouse-server

echo "=== Verifying ClickHouse installation ==="
sudo systemctl is-active clickhouse-server
clickhouse-client --version

echo "=== ClickHouse installation complete! ==="