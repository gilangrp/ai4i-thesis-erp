#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "=== Updating system packages ==="
sudo apt update && sudo apt upgrade -y

echo "=== Installing prerequisite common package ==="
# This package provides the official pgdg setup script
sudo apt install -y postgresql-client-common

echo "=== Running official PostgreSQL repository script ==="
# Automatically sets up the PGDG repository and signing keys for your Ubuntu version
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh

echo "=== Updating package list with new repository ==="
sudo apt update

echo "=== Installing PostgreSQL Server and Extra Utilities ==="
# Installs the latest version of PostgreSQL and common additional extensions
sudo apt install -y postgresql postgresql-contrib

echo "=== Verifying Installation Status ==="
sudo systemctl is-active postgresql
psql --version

echo "=== PostgreSQL installation complete! ==="
