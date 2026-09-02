import json
import os
from datetime import datetime, timedelta

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
APPIDS = [767320, 2313020]

HOST_PROJECT_DIR = os.environ["HOST_PROJECT_DIR"]
PARQUET_GLOB = "/opt/airflow/data/raw/dt=*/appid=*/*.parquet"
DUCKDB_FILE = "/opt/airflow/duckdb_data/mydb.duckdb"
SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")


def load_sql(filename: str) -> str:
    """Reads a .sql file from dags/sql/ as plain text."""
    with open(os.path.join(SQL_DIR, filename), "r") as f:
        return f.read()


def get_watermarks(**context):
    con = duckdb.connect(DUCKDB_FILE)
    try:
        con.execute(load_sql("create_pipeline_watermarks.sql"))

        watermarks = {}
        for appid in APPIDS:
            row = con.execute(
                "SELECT last_timestamp, reviewids_at_last FROM pipeline_watermarks WHERE appid = ?",
                [appid],
            ).fetchone()
            if row is None:
                watermarks[str(appid)] = {"last_timestamp": 0, "reviewids": []}
            else:
                watermarks[str(appid)] = {
                    "last_timestamp": row[0],
                    "reviewids": json.loads(row[1]),
                }

        print("Watermarks read:", watermarks)
        return watermarks
    finally:
        con.close()


def load_to_duckdb(**context):

    con = duckdb.connect(DUCKDB_FILE)
    try:
        con.execute(load_sql("create_steam_reviews.sql"))

        con.execute(f"""
            INSERT INTO steam_reviews
            SELECT
                reviewid,
                appid,
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

        for appid in APPIDS:
            max_ts = con.execute(
                "SELECT MAX(timestamp_created) FROM steam_reviews WHERE appid = ?", [appid]
            ).fetchone()[0]

            if max_ts is None:
                continue  

            ids_at_max = con.execute(
                "SELECT reviewid FROM steam_reviews WHERE appid = ? AND timestamp_created = ?",
                [appid, max_ts],
            ).fetchall()
            ids_at_max = [r[0] for r in ids_at_max]

            con.execute("""
                INSERT INTO pipeline_watermarks VALUES (?, ?, ?)
                ON CONFLICT (appid) DO UPDATE SET
                    last_timestamp = excluded.last_timestamp,
                    reviewids_at_last = excluded.reviewids_at_last
            """, [appid, int(max_ts.timestamp()), json.dumps(ids_at_max)])

            print(f"  appid={appid}: watermark advanced to ts={int(max_ts.timestamp())} "
                  f"({len(ids_at_max)} id(s) at that ts)")
    finally:
        con.close()


default_args = {
    "owner": "de-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="steam-reviews",
    default_args=default_args,
    start_date=datetime(2026, 7, 23),
    schedule="@weekly",
    catchup=False,
) as dag:

    get_watermarks_task = PythonOperator(
        task_id="get_watermarks",
        python_callable=get_watermarks,
    )

    scrape_reviews = DockerOperator(
        task_id="scrape_reviews",
        image="steamreview:1.0",
        command=["python", "main.py"],
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "WATERMARKS_JSON": "{{ ti.xcom_pull(task_ids='get_watermarks') | tojson }}",
        },
        mounts=[
            Mount(source=f"{HOST_PROJECT_DIR}/data", target="/app/data", type="bind"),
        ],
        mount_tmp_dir=False,
    )

    load_to_duckdb_task = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_to_duckdb,
    )

    get_watermarks_task >> scrape_reviews >> load_to_duckdb_task