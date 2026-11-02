# 1. Cari file konfigurasi
sudo -u postgres psql -c "SHOW config_file;"
sudo -u postgres psql -c "SHOW hba_file;"

# 2. Ubah listen_addresses di postgresql.conf:
listen_addresses = '*'

# 3. Izinkan user di pg_hba.conf (tambahkan di bawah):
hostssl  erp_ai4i  airflow_etl  0.0.0.0/0  scram-sha-256
#0.0.0.0/0 dipakai karena IP runner GitHub berubah-ubah. Karena itu, pastikan password airflow_etl kuat dan user-nya hanya punya hak SELECT di database yang dibutuhkan.

# 4. Restart
sudo systemctl restart postgresql
# ss -tlnp | grep 5432  # harus jadi 0.0.0.0:5432

# 5. Tes dari luar (dari laptop)
nc -zv <IP_VPS> 5432