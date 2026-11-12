# 1. Create folder ~/airflow/dags
mkdir -p ~/airflow/dags

# 2. Create symlink from project DAG file to Airflow dags folder
ln -s "$(pwd)/dags/etl_postgre_to_clickhouse_mfp.py" ~/airflow/dags/
