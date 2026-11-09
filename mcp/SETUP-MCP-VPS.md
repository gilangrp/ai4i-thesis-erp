# Setup MCP di VPS dengan Apache + SSL

Dokumen ini berisi langkah cepat untuk menyiapkan reverse proxy Apache agar domain dapat mengarahkan request ke aplikasi MCP yang sedang berjalan di `127.0.0.1:8000`, lalu menambahkan SSL dengan Certbot.

## 1) Aktifkan modul Apache yang dibutuhkan

```bash
sudo a2enmod proxy
sudo a2enmod proxy_http
sudo a2enmod ssl
sudo a2enmod headers
sudo a2enmod rewrite
```

Lalu cek konfigurasi Apache:

```bash
sudo apachectl configtest
```

Jika berhasil, output yang diharapkan adalah:

```text
Syntax OK
```

## 2) Buat VirtualHost untuk HTTP

```bash
sudo nano /etc/apache2/sites-available/erp.conf
```

Masukkan konfigurasi berikut:

```apache
<VirtualHost *:80>
    ServerName erp.gilangrizki.my.id

    ProxyPreserveHost On
    ProxyRequests Off

    ProxyPass / http://127.0.0.1:8000/
    ProxyPassReverse / http://127.0.0.1:8000/

    ErrorLog ${APACHE_LOG_DIR}/erp-error.log
    CustomLog ${APACHE_LOG_DIR}/erp-access.log combined
</VirtualHost>
```

Aktifkan konfigurasi site:

```bash
sudo a2ensite erp.conf
sudo systemctl reload apache2
```

## 3) Verifikasi VirtualHost dan HTTP

Cek apakah domain sudah terdaftar di Apache:

```bash
sudo apachectl -S
```

Pastikan `erp.gilangrizki.my.id` muncul sebagai VirtualHost di port 80.

Tes akses HTTP:

```bash
curl -I http://erp.gilangrizki.my.id
```

Jika server lokal sudah berjalan di port 8000, response HTTP yang muncul seharusnya berhasil (misalnya `200 OK` atau `301/302` sesuai konfigurasi).

## 4) Jalankan Certbot untuk SSL

Setelah HTTP aktif, jalankan:

```bash
sudo certbot --apache -d erp.gilangrizki.my.id
```

Certbot akan melakukan hal berikut:

1. Memvalidasi kepemilikan domain.
2. Meminta sertifikat SSL dari Let's Encrypt.
3. Memasang sertifikat ke Apache.
4. Mengonfigurasi VirtualHost HTTPS di port 443.

Jika diminta, pilih opsi untuk redirect HTTP ke HTTPS.

## 5) Verifikasi sertifikat SSL

Cek sertifikat yang terpasang:

```bash
sudo certbot certificates
```

Output yang diharapkan berisi sesuatu seperti:

```text
Certificate Name: erp.gilangrizki.my.id
Domains: erp.gilangrizki.my.id
Expiry Date: ...
```

Tes HTTPS:

```bash
curl -Iv https://erp.gilangrizki.my.id
```

Jika semua langkah berhasil, domain Anda sudah bisa diakses melalui HTTPS dan dialihkan dari HTTP ke HTTPS.

## Catatan

- Pastikan port 80 dan 443 terbuka di firewall/server.
- Jika aplikasi MCP ada di port lain, sesuaikan `ProxyPass` dan `ProxyPassReverse`.
- Jika `certbot` gagal, pastikan DNS domain sudah mengarah ke VPS dan Apache sudah berjalan dengan benar.
