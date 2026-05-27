from include.lesmills_project.run import snapshot as snapshot_fn
from src.les.prediction.readdata import read_data as read_data_fn
from src.les.prediction.predict import run_prediction
from src.les.prediction.ingets import ingest_data
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import get_current_context
from airflow import DAG
from airflow.sdk import task
from datetime import datetime
from pathlib import Path
import subprocess
import pandas as pd


DATA_DIR = Path("/usr/local/airflow/include/data/predict/")
DATA_DIR.mkdir(parents=True, exist_ok=True)


with DAG(
    dag_id="Prediction",
    description="Les Mills retention pipeline",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:


    # ---------------- READ DATA ----------------
    @task
    def read_data_task():
        return read_data_fn()


    # ---------------- SNAPSHOT ----------------
    @task
    def snapshots_task(df):
        return snapshot_fn(df)

    







    # ---------------- INGEST ----------------
    @task
    def ingest_task(snap_df):
        return ingest_data(snap_df)


    # ---------------- PREDICT ----------------
    @task
    def predict_task(ingest_result):

        merged_path = ingest_result["df_merged_path"]

        df = pd.read_parquet(merged_path)

        df_pred = run_prediction(df)

        return df_pred


    # ---------------- FINAL MERGE ----------------
    @task
    def final_merge(ingest_result, df_pred):
        context = get_current_context()
        run_date = context["ds"]
        sql_path = ingest_result["df_sql_path"]

        df_sql = pd.read_parquet(sql_path)

        df_final = df_sql.merge(
            df_pred,
            on="MembershipID",
            how="inner"
        )
        df_final["RunDate"]=run_date
        df_final.columns = df_final.columns.str.strip()
        print(df_final.columns)
        df_final=df_final[["RunDate",
                "MembershipID",
                "SubCategory",
                "RegularPayment",
                "Gender",
                "Age",
                "TotalAttendance",
                "prediction",
                "confidence"
                ]]
        output_path = DATA_DIR / "final.csv"

        df_final.to_csv(output_path, index=False)

        return df_final

    # upsert into sql server table
    @task
    def upsert_to_sql(df):
        import numpy as np

        # clean column names
        df.columns = df.columns.str.strip()

        # rename prediction columns if needed
        df = df.rename(columns={
            "prediction": "Prediction",
            "confidence": "PredictionConfidence"
        })

        # convert NaN -> None (SQL NULL)
        df = df.replace({np.nan: None})
        df = df[df["SubCategory"] != "Complimentary"]
        run_date = df["RunDate"].iloc[0]

        hook = MsSqlHook(mssql_conn_id="mssql")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            "DELETE FROM repo.MembershipRetentionPredictions WHERE RunDate = %s",
            (run_date,)
        )

        insert_query = """
            INSERT INTO repo.MembershipRetentionPredictions (
                RunDate,
                MembershipID,
                SubCategory,
                RegularPayment,
                Gender,
                Age,
                TotalAttendance,
                Prediction,
                PredictionConfidence
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        rows = df[
            [
                "RunDate",
                "MembershipID",
                "SubCategory",
                "RegularPayment",
                "Gender",
                "Age",
                "TotalAttendance",
                "Prediction",
                "PredictionConfidence"
                
            ]
        ].values.tolist()

        cursor.executemany(insert_query, rows)

        conn.commit()

        cursor.close()
        conn.close()

    # ---------------- DAG FLOW ----------------

    raw_df = read_data_task()

    snap_df = snapshots_task(raw_df)

    ingest_result = ingest_task(snap_df)

    pred_df = predict_task(ingest_result)

    final_output = final_merge(ingest_result, pred_df)

    upsert=upsert_to_sql(final_output)

    upsert 

    

    