<<<<<<< HEAD
from include.lesmills_project.run import snapshot as snapshot_fn
from include.lesmills_project.ingest import read_data as read_data_fn
from src.les.ingets import ingest_data, transform_data, train_model
import subprocess
from airflow import DAG
from airflow.sdk import task  # updated for deprecation
from datetime import datetime
from pathlib import Path

DATA_DIR = Path("/usr/local/airflow/include/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

with DAG(
    dag_id='lesmills',
    description='Les Mills retention pipeline',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
=======

from include.lesmills_project.final import evaluate as evaluate_fn
from include.lesmills_project.run import snapshot as snapshot_fn
from include.lesmills_project.ingest import read_data as read_data_fn
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.utils.edgemodifier import Label
import time
with DAG(
    dag_id='lesmills',
    description='Les Mills retention pipeline',
    schedule='@daily',               # or '@monthly' etc.
    start_date=datetime(2025, 1, 1), # DAG will still show even if this is in the past
>>>>>>> 34f3a5f24a0abcc543b1c34e6fe2aca904298887
    catchup=False
) as dag:

    @task
    def read_data_task():
<<<<<<< HEAD
        df = read_data_fn()
        return df

    @task
    def snapshots_task(df):
        snap_df = snapshot_fn(df)
        return snap_df
    @task
    def merge_task(snap_df):
        df = ingest_data(snap_df)
        path = DATA_DIR / "merged_data.csv"
        df.to_csv(path, index=False)
        return str(path)

    # foloinwg task is just when we want to train the model, we can comment it out when we just want to run the streamlit app
    # @task
    # def ingest_transform_train(snap_df):
    #     df = ingest_data(snap_df)
    #     split_data = transform_data(df)
    #     train_model(*split_data)
       

    
    @task
    def streamlit_app():
        subprocess.Popen(
            [
                "streamlit",
                "run",
                "/usr/local/airflow/include/lesmills_project/streamlit.py",
                "--server.port=8501",
                "--server.address=0.0.0.0"
            ]
        )
    # TaskFlow API chaining
    raw_df = read_data_task()
    snap_df = snapshots_task(raw_df)
    merged = merge_task(snap_df)

    merged >> streamlit_app()
=======
        # this calls ingest.read_data()
        read_data_fn()
    @task
    def sleeping_first():
        time.sleep(60)
    @task
    def snapshots_task():
        # this calls run.snapshot()
        snapshot_fn()
    @task
    def sleeping_second():
        time.sleep(60)
    @task
    def evaluate_task():
        # this calls final.evaluate()
        evaluate_fn()

    # # task dependencies
    read_data_task() >> sleeping_first()>> snapshots_task() >>sleeping_second()>> evaluate_task()
>>>>>>> 34f3a5f24a0abcc543b1c34e6fe2aca904298887
