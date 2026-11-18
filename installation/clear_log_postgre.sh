# Hapus semua file arsip log lama:
sudo rm -f /var/log/postgresql/*.log.* /var/log/postgresql/*.gz

# Kosongkan semua file log aktif yang sedang berjalan:
sudo truncate -s 0 /var/log/postgresql/*.log

# Verifikasi hasil:
ls -lh /var/log/postgresql/

scp root@202.10.42.115:/var/log/postgresql/postgresql-14-main.log.1 .
