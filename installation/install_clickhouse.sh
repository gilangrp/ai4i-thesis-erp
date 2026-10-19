
#!/usr/bin/env bash
set -euo pipefail

echo "=== Update package ==="
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg

echo "=== Add ClickHouse repository key ==="
sudo install -d -m 0755 /usr/share/keyrings

curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key \
  | sudo gpg --dearmor --yes \
      -o /usr/share/keyrings/clickhouse-keyring.gpg

echo "=== Add ClickHouse repository ==="
ARCH="$(dpkg --print-architecture)"

echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=${ARCH}] https://packages.clickhouse.com/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/clickhouse.list >/dev/null

echo "=== Install ClickHouse ==="
sudo apt-get update
sudo apt-get install -y clickhouse-server clickhouse-client

echo "=== Enable and start service ==="
sudo systemctl enable --now clickhouse-server

echo "=== Check version ==="
clickhouse-client --query "SELECT version()"

echo "=== Check service status ==="
sudo systemctl --no-pager --full status clickhouse-server

echo "=== Check RAM and disk ==="
free -h
df -h /

echo "=== ClickHouse installation completed ==="
