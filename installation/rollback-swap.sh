
#!/bin/bash
set -e

echo "=== Rollback Swap: 2 GB -> 256 MiB ==="

# Pastikan backup tersedia
if [ ! -f /swapfile.backup ]; then
    echo "ERROR: Backup swap lama tidak ditemukan!"
    exit 1
fi

# Matikan swap 2 GB
sudo swapoff /swapfile

# Hapus swap baru
sudo rm -f /swapfile

# Kembalikan swap lama
sudo cp --preserve=all /swapfile.backup /swapfile

# Aktifkan kembali
sudo swapon /swapfile

echo "=== Rollback Complete ==="
free -h
swapon --show
