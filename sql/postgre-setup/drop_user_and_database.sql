-- ==============================================================================
-- CLEANUP / TEARDOWN: DROP USER & PERMISSIONS: POSTGRESQL (airflow_etl)
-- Eksekusi sebagai superuser: postgres
-- ==============================================================================

-- 1. Putuskan semua sesi/koneksi aktif milik user airflow_etl (jika ada)
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE usename = 'airflow_etl'
  AND pid <> pg_backend_pid();

-- 2. Pindah ke database erp_ai4i untuk mencabut privilege objek di dalamnya
\c erp_ai4i

-- 3. Cabut Default Privileges yang pernah diberikan di masa depan
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL PRIVILEGES ON TABLES FROM airflow_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL PRIVILEGES ON SEQUENCES FROM airflow_etl;

-- 4. Cabut hak akses pada tabel, sequence, dan schema public
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM airflow_etl;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM airflow_etl;
REVOKE ALL PRIVILEGES ON SCHEMA public FROM airflow_etl;

-- 5. Reassign objek yang mungkin dibuat oleh airflow_etl ke user postgres & drop dependensi objek
REASSIGN OWNED BY airflow_etl TO postgres;
DROP OWNED BY airflow_etl;

-- 6. Pindah kembali ke database postgres
\c postgres

-- 7. Cabut hak akses level Database erp_ai4i
REVOKE ALL PRIVILEGES ON DATABASE erp_ai4i FROM airflow_etl;
REVOKE CONNECT ON DATABASE erp_ai4i FROM airflow_etl;

-- 8. Reassign dan drop dependensi objek di database postgres (jika ada)
REASSIGN OWNED BY airflow_etl TO postgres;
DROP OWNED BY airflow_etl;

-- 9. Hapus user airflow_etl
DROP USER IF EXISTS airflow_etl;

-- ==============================================================================
-- 10. (OPSIONAL) Hapus Database erp_ai4i jika tidak lagi digunakan
-- Uncomment baris di bawah jika ingin menghapus databasenya sekaligus:
--
-- SELECT pg_terminate_backend(pid)
-- FROM pg_stat_activity
-- WHERE datname = 'erp_ai4i' AND pid <> pg_backend_pid();
--
-- DROP DATABASE IF EXISTS erp_ai4i;
-- ==============================================================================

-- 11. Validasi / Verifikasi apakah user airflow_etl sudah berhasil dihapus
-- (Jika berhasil, query ini akan mengembalikan 0 baris)
SELECT rolname, rolsuper, rolcanlogin
FROM pg_catalog.pg_roles
WHERE rolname = 'airflow_etl';

