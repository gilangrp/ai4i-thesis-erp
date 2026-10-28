-- CREATE DATABASE login as postgres
CREATE DATABASE erp_ai4i;
\l
\c erp_ai4i


-- login as postgres -> database erp_ai4i
SELECT current_database(), current_user;
GRANT CREATE ON SCHEMA public TO airflow_etl;
GRANT USAGE ON SCHEMA public TO airflow_etl;

SELECT
    has_schema_privilege(
        'airflow_etl',
        'public',
        'CREATE'
    ) AS has_create;