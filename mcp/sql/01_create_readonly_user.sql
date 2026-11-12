-- Jalankan sebagai admin PostgreSQL, terhubung ke database yang benar.
-- Ganti: nama_database, schema, dan password.

CREATE ROLE mcp_machine_operation_readonly
  WITH LOGIN PASSWORD 'GANTI_PASSWORD_KUAT';

GRANT CONNECT ON DATABASE nama_database
  TO mcp_machine_operation_readonly;

GRANT USAGE ON SCHEMA public
  TO mcp_machine_operation_readonly;

-- Hanya SELECT, hanya pada tabel ini.
GRANT SELECT ON TABLE public.machine_operation
  TO mcp_machine_operation_readonly;

-- Pengaman tambahan di level role.
ALTER ROLE mcp_machine_operation_readonly SET default_transaction_read_only = on;
ALTER ROLE mcp_machine_operation_readonly SET statement_timeout = '15s';

-- Verifikasi (opsional):
-- \c nama_database mcp_machine_operation_readonly
-- SELECT * FROM public.machine_operation LIMIT 1;   -- harus berhasil
-- DELETE FROM public.machine_operation;             -- harus ditolak
