-- ==============================================================================
-- CLEANUP / TEARDOWN: DROP USER & DATABASE: CLICKHOUSE
-- Modul: Machine Failure Prediction (MFP)
-- Database: erp_ai4i
-- User: airflow_etl
-- Eksekusi sebagai admin / user: default
-- ==============================================================================

-- 1. Cabut hak akses (Permissions & Engine) dari user airflow_etl
REVOKE ALL ON erp_ai4i.* FROM airflow_etl;
REVOKE TABLE ENGINE ON MergeTree FROM airflow_etl;

-- 2. Hapus User airflow_etl
DROP USER IF EXISTS airflow_etl;

-- 3. Hapus Database erp_ai4i beserta seluruh tabel di dalamnya
-- (SYNC memastikan proses penghapusan data di disk selesai sebelum query kembali)
DROP DATABASE IF EXISTS erp_ai4i SYNC;

-- 4. Validasi / Verifikasi Penghapusan
-- (Jika berhasil, kedua query ini tidak akan menampilkan hasil / kosong)
SHOW USERS LIKE 'airflow_etl';
SHOW DATABASES LIKE 'erp_ai4i';

-- Alternatif validasi via system tables:
-- SELECT name, auth_type FROM system.users WHERE name = 'airflow_etl';
-- SELECT name FROM system.databases WHERE name = 'erp_ai4i';

-- ==============================================================================
-- CARA EKSEKUSI VIA CLI:
-- clickhouse client --multiquery < drop_user_and_database.sql
-- atau:
-- clickhouse-client --multiquery < drop_user_and_database.sql
-- ==============================================================================

