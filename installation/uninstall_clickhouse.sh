sudo systemctl stop clickhouse-server
sudo systemctl disable clickhouse-server

sudo apt purge -y clickhouse-server clickhouse-client clickhouse-common-static

sudo apt autoremove -y

sudo rm -rf /var/lib/clickhouse
sudo rm -rf /var/log/clickhouse-server
sudo rm -rf /etc/clickhouse-server

sudo apt clean
sudo apt update

# REINSTALL
sudo apt install -y clickhouse-server clickhouse-client