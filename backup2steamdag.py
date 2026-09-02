from airflow import DAG
from airflow.providers.operators.docker import DockerOperator
from airflow.provider.operators import PythonOperator
from datetime import datetime,timedelta
from docker.types import Mount
import os
import duckdb
HOST_PROJECT_DIR = os.environ["HOST_PROJECT_DIR"]
PARQUET_GLOB = "/opt/airflow/data/raw/dt=*/appid=*/*.parquet"
DUCKDB_FILE = "/opt/airflow/duckdb_data/mydb.duckdb"
def load_to_duckdb():
    con = duckdb.connect(DUCKDB_FILE)
    try:
        con.execute("""
                CREATE TABLE IF NOT EXISTS steam_reviews (
                    reviewid VARCHAR PRIMARY KEY,
                    appid BIGINT,
                    review VARCHAR,
                    language VARCHAR,
                    timestamp_created TIMESTAMPTZ,
                    refunded BOOLEAN,
                    extracted_at VARCHAR,
                    game_name VARCHAR,
                    publisher VARCHAR
                )
            """)

        result = con.execute(f"""
                INSERT INTO steam_reviews
                SELECT
                    reviewid,
                    appid
                    review,
                    language,
                    timestamp_created,
                    refunded,
                    extracted_at,
                    game_name,
                    publisher
                FROM read_parquet('{PARQUET_GLOB}', hive_partitioning=true)
                ON CONFLICT (reviewid) DO NOTHING
            """)
 
        total = con.execute("SELECT COUNT(*) FROM steam_reviews").fetchone()[0]
        print(f"Load complete. steam_reviews now has {total} total rows.")
    finally:
        con.close()
 



default_args = {
    "owner":"de-team",
    "retries":3,
    "retry_delay":timedelta(minutes=5)
   
}

with DAG(
    dag_id="steam-reviews",
    default_args=default_args,
    start_date=datetime(2026,7,23),
    schedule_interval= '@weekly',
    catchup=False
)as dag:
    run_incremental_load_steam_review = DockerOperator(
        task_id = "incremental_load_review",
        image = "steamreview:1.0",
        command = "python main.py",
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",

        mounts=[
            Mount(
                sources=f"{HOST_PROJECT_DIR}/data", 
                target="/app/data", 
                type="bind"
            ),
        ],
        mount_tmp_dir=False

    )
    load_to_duckdb_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_to_duckdb,
    )
 
run_incremental_load_steam_review >> load_to_duckdb_task
