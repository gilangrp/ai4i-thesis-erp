# mcp-machine-operation

MCP Server **read-only** untuk tabel PostgreSQL `machine_operation`, agar Claude dapat membaca data operasi mesin lewat Model Context Protocol (Streamable HTTP).

```text
User -> Claude (MCP client) -> HTTPS (Caddy) -> MCP Server -> PostgreSQL (read-only)
```

## Struktur

```text
main.py                        # MCP server + auth bearer token + /health
requirements.txt
Dockerfile
compose.yaml                   # mcp server + Caddy (HTTPS otomatis)
Caddyfile
.env.example
sql/01_create_readonly_user.sql
```

## Tools

| Tool | Fungsi |
|---|---|
| `get_table_schema` | Daftar kolom & tipe data tabel |
| `get_machine_operations` | Ambil record (limit + offset) |
| `get_machine_operations_by_date` | Filter rentang tanggal (`today`, `yesterday`, `YYYY-MM-DD`) |
| `count_machine_operations` | Hitung record, opsional per tanggal |

Semua query memakai parameter/identifier aman, tidak ada SQL bebas dari AI.

## Setup

1. **Buat user DB read-only**: jalankan `sql/01_create_readonly_user.sql` sebagai admin.
2. **Izinkan container mengakses Postgres di host** (jika Postgres di VPS yang sama):
   - `postgresql.conf`: `listen_addresses = 'localhost,172.17.0.1'`
   - `pg_hba.conf`: `host nama_database mcp_machine_operation_readonly 172.16.0.0/12 scram-sha-256`
   - Firewall: port 5432 **jangan** dibuka ke internet.
3. **Konfigurasi**:
   ```bash
   cp .env.example .env
   openssl rand -hex 32   # isi ke MCP_AUTH_TOKEN
   nano .env              # isi DOMAIN, POSTGRES_*, dst.
   ```
4. **Jalankan**:
   ```bash
   docker compose up -d --build
   docker compose logs -f mcp-machine-operation
   ```
   Untuk menjalankan tanpa Docker, dari root repository:
   ```bash
   cd mcp
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   python main.py
   ```
   Pastikan `.env` sudah dikonfigurasi. Jika PostgreSQL berjalan di mesin yang sama, atur `POSTGRES_HOST=127.0.0.1` (bukan `host.docker.internal`).
5. **Uji**:
   ```bash
   curl https://DOMAIN/health
   curl -i https://DOMAIN/mcp            # harus 401 tanpa token
   ```
6. **Setelah skema diketahui**: panggil `get_table_schema`, lalu isi `ORDER_BY_COLUMN` dan `DATE_COLUMN` di `.env`, kemudian `docker compose up -d`.

## Menghubungkan ke Claude

URL endpoint: `https://DOMAIN/mcp`

- **Claude Code / API MCP connector**: mendukung header/token, jadi bearer token langsung dipakai. Contoh Claude Code:
  `claude mcp add --transport http machine-operation https://DOMAIN/mcp --header "Authorization: Bearer TOKEN"`
- **Claude web/desktop (Settings → Connectors → Add custom connector)**: setahu saya UI ini mendukung OAuth, bukan header bearer statis. Pilihan: (a) tambahkan OAuth di server, atau (b) batasi akses di firewall/reverse proxy ke IP yang dibutuhkan. Cek dokumentasi Claude terbaru untuk opsi autentikasi yang tersedia.

## Catatan keamanan

- Jangan commit `.env`. Jangan taruh password DB di konfigurasi Claude.
- Gunakan `POSTGRES_SSLMODE=require`/`verify-full` bila DB di mesin lain.
- Data hasil query dikirim ke model; pastikan tabel tidak berisi data sensitif yang tidak boleh keluar.
- Endpoint belum siap produksi sebelum autentikasi sesuai klien Claude yang dipakai diverifikasi.

## Checklist

- [ ] Verifikasi database/schema/tabel
- [ ] User read-only dibuat & diuji (DELETE harus ditolak)
- [ ] `.env` diisi, token acak dibuat
- [ ] Domain mengarah ke VPS, port 80/443 terbuka
- [ ] `docker compose up -d --build` sukses, `/health` OK
- [ ] `ORDER_BY_COLUMN` & `DATE_COLUMN` diisi
- [ ] Terhubung ke Claude dan tool diuji
