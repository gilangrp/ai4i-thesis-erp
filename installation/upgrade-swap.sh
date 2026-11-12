
#!/bin/bash
set -e

echo "=== Upgrade Swap: 256 MiB -> 2 GB ==="

# Backup swap lama
if [ ! -f /swapfile.backup ]; then
    sudo swapoff /swapfile
    sudo cp --preserve=all /swapfile /swapfile.backup
else
    echo "Backup sudah tersedia."
    sudo swapoff /swapfile
fi

# Buat swap baru 2 GB
sudo rm -f /swapfile
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile

# Aktifkan swap baru
sudo swapon /swapfile

echo "=== Upgrade Complete ==="
free -h
swapon --show
