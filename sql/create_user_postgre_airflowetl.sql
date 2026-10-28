CREATE USER airflow_etl WITH PASSWORD 'airflow_etl';
GRANT CONNECT ON DATABASE erp_ai4i TO airflow_etl;
GRANT USAGE ON SCHEMA public TO airflow_etl;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_etl;