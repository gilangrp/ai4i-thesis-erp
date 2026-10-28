-- 1. Create user
CREATE USER IF NOT EXISTS airflow_etl
IDENTIFIED BY 'airflow_etl';

-- 2. Grant access to database
GRANT ALL ON erp_ai4i.* TO airflow_etl;

-- 3. Grant permission to use MergeTree engine
GRANT TABLE ENGINE ON MergeTree TO airflow_etl;

-- 4. Validate permissions
SHOW GRANTS FOR airflow_etl;

-- Validation .sh
clickhouse-client --user airflow_etl --password airflow_etl --database erp_ai4i
