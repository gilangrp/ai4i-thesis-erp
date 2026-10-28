-- 1. Create user with password
CREATE USER IF NOT EXISTS airflow_etl IDENTIFIED BY 'airflow_etl';

-- 2. Grant access
GRANT ALL ON erp_ai4i.* TO airflow_etl;

-- Validation
clickhouse-client --user airflow_etl --password airflow_etl --database erp_ai4i
