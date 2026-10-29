-- ==============================================================================
-- SETUP DATABASE & USER PERMISSIONS: POSTGRESQL (erp_ai4i)
-- Eksekusi sebagai user: postgres
-- ==============================================================================

-- 1. Buat Database erp_ai4i 
CREATE DATABASE erp_ai4i;

-- 2. Buat User airflow_etl 
DO
$do$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow_etl') THEN
      CREATE USER airflow_etl WITH PASSWORD 'airflow_etl';
   ELSE
      ALTER USER airflow_etl WITH PASSWORD 'airflow_etl';
   END IF;
END
$do$;

-- 3. Pindah ke database erp_ai4i (PENTING: seluruh hak akses objek di bawah berlaku di sini)
\c erp_ai4i

-- 4. Hak Akses Level Database
GRANT CONNECT ON DATABASE erp_ai4i TO airflow_etl;

-- 5. Hak Akses Level Schema public
GRANT USAGE, CREATE ON SCHEMA public TO airflow_etl;

-- 6. Hak Akses Level Tabel & Sequence yang SUDAH ADA
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_etl;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_etl;

-- 7. Hak Akses Default untuk Tabel & Sequence yang DIBUAT DI MASA DEPAN
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO airflow_etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO airflow_etl;

-- 8. Validasi Hak Akses User airflow_etl
SELECT current_database(), current_user;
SELECT
    has_database_privilege('airflow_etl', 'erp_ai4i', 'CONNECT') AS can_connect_db,
    has_schema_privilege('airflow_etl', 'public', 'USAGE') AS can_use_schema,
    has_schema_privilege('airflow_etl', 'public', 'CREATE') AS can_create_schema;
