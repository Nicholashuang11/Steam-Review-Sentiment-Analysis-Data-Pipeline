from airflow import DAG
from airflow.providers.operators.docker import DockerOperator
from airflow.provider.operators import PythonOperator
from datetime import datetime,timedelta
from docker.types import Mount
import os
import duckdb
APPIDS = [767320, 2313020]
HOST_PROJECT_DIR = os.environ["HOST_PROJECT_DIR"]
PARQUET_GLOB = "/opt/airflow/data/raw/dt=*/appid=*/*.parquet"
DUCKDB_FILE = "/opt/airflow/duckdb_data/mydb.duckdb"
SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
def load_sql(filename: str) -> str:
    with open(os.path.join(SQL_DIR, filename), "r") as f:
        return f.read()

def load_to_duckdb():
    con = duckdb.connect(DUCKDB_FILE)
    try:
        con.execute(load_sql("create_steam_reviews.sql"))
        con.execute(f"""
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
    
        for appid in APPIDS:
                    max_ts = con.execute(
                        "SELECT MAX(timestamp_created) FROM steam_reviews WHERE appid = ?", [appid]
                    ).fetchone()[0]
        
                    if max_ts is None:
                        continue  # no data for this game yet at all
        
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

 

def get_watermark_task(**context):
    con = duckdb.connect(DUCKDB_FILE)
    try:
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
        print("watermarks read:",watermarks)
        return watermarks
    finally:
         con.close9


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
