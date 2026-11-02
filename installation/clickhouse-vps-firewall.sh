# clickhouse config file
nano /etc/clickhouse-server/config.xml

# find <listen_host>. and change it to:
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
</clickhouse>

sudo systemctl restart clickhouse-server
sudo ufw allow 8123/tcp