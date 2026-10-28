-- =========================================================
-- DELETE USER: airflow_etl
-- Database: erp_ai4i
-- =========================================================

-- 1. Cabut privilege database
REVOKE CONNECT
ON DATABASE erp_ai4i
FROM airflow_etl;

-- 2. Cabut privilege schema
REVOKE USAGE, CREATE
ON SCHEMA public
FROM airflow_etl;

-- 3. Cabut privilege seluruh tabel
REVOKE ALL PRIVILEGES
ON ALL TABLES IN SCHEMA public
FROM airflow_etl;

-- 4. Cabut privilege seluruh sequence
REVOKE ALL PRIVILEGES
ON ALL SEQUENCES IN SCHEMA public
FROM airflow_etl;

-- 5. Hapus user
DROP USER airflow_etl;