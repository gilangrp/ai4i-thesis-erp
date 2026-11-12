#!/bin/bash

# Clear screen untuk tampilan yang bersih
clear

echo "=================================================="
echo "          INFORMASI SPESIFIKASI SISTEM            "
echo "=================================================="

# 1. INFORMASI OS
echo -e "\n[1] Informasi OS & Kernel:"
echo "----------------------------------------"
if [ -f /etc/os-release ]; then
    . /etc/os-release
    echo "Nama OS     : $NAME"
    echo "Versi OS    : $VERSION"
else
    echo "Nama OS     : Tidak diketahui"
fi
echo "Kernel      : $(uname -r)"
echo "Arsitektur  : $(uname -m)"
echo "Hostname    : $(hostname)"

# 2. INFORMASI CPU
echo -e "\n[2] Informasi CPU:"
echo "----------------------------------------"
CPU_MODEL=$(lscpu | grep "Model name:" | sed 's/Model name:[ \t]*//')
CPU_CORES=$(lscpu | grep "^CPU(s):" | sed 's/CPU(s):[ \t]*//')
echo "Model CPU   : ${CPU_MODEL:-Virtual/Generic CPU}"
echo "Jumlah Core : $CPU_CORES"

# 3. INFORMASI RAM
echo -e "\n[3] Informasi Memori (RAM):"
echo "----------------------------------------"
# Menggunakan free -h untuk format gigabyte/megabyte yang mudah dibaca
free -h | awk '
  /Mem:/ {print "Total RAM   : " $2 "\nTerpakai    : " $3 "\nTersedia    : " $7}
'

# 4. INFORMASI STORAGE (PENYIMPANAN)
echo -e "\n[4] Informasi Penyimpanan (Storage Root /):"
echo "----------------------------------------"
# Mengambil info partisi root (/)
df -h / | awk '
  NR==2 {print "Total Disk  : " $2 "\nTerpakai    : " $3 " (" $5 ")\nTersedia    : " $4}
'

echo "=================================================="
