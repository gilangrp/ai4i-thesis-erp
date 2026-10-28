-- =========================================================
-- ADD USER: airflow_etl
-- Database: erp_ai4i
-- =========================================================

-- 1. Buat user
CREATE USER airflow_etl
WITH PASSWORD 'airflow_etl';

-- 2. Izinkan user connect ke database
GRANT CONNECT
ON DATABASE erp_ai4i
TO airflow_etl;

-- 3. Izinkan akses dan membuat object di schema public
GRANT USAGE, CREATE
ON SCHEMA public
TO airflow_etl;

-- 4. Berikan privilege penuh pada tabel yang SUDAH ADA
GRANT ALL PRIVILEGES
ON ALL TABLES IN SCHEMA public
TO airflow_etl;

-- 5. Berikan privilege penuh pada sequence yang SUDAH ADA
GRANT ALL PRIVILEGES
ON ALL SEQUENCES IN SCHEMA public
TO airflow_etl;

-- 6. Agar tabel/sequence BARU otomatis bisa diakses airflow_etl
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL PRIVILEGES ON TABLES
TO airflow_etl;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL PRIVILEGES ON SEQUENCES
TO airflow_etl;