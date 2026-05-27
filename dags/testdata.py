from src.les.testtrain.run import snapshot as snapshot_fn
from src.les.testtrain.read_data import read_data as read_data_fn
from src.les.testtrain.predict import run_prediction
from src.les.testtrain.ingest import ingest_data
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import get_current_context
from airflow import DAG
from airflow.sdk import task
from datetime import datetime
from pathlib import Path
import subprocess
import pandas as pd
from airflow.models import Variable

DATA_DIR = Path("/usr/local/airflow/include/data/testresult/")
DATA_DIR.mkdir(parents=True, exist_ok=True)


with DAG(
    dag_id="Testmodel",
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
        run_date = pd.to_datetime(Variable.get("test_date")).strftime("%Y-%m-%d")
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
                "confidence",
                "EndDate"]]
        output_path = DATA_DIR / "final.csv"

        df_final.to_csv(output_path, index=False)

        return df_final

    # ---------------- EVALUATE ----------------
    @task
    def evaluate_task(df):
        """
        Derive the true class from EndDate (same buckets as the training label)
        and report precision/recall/F1 per class against the model's Prediction.
        Encoded classes: 0 = imminent (0-3mo), 1 = medium (3-6mo), 2 = stable (else).
        """
        import logging
        import numpy as np
        from sklearn.metrics import (
            confusion_matrix,
            classification_report,
            precision_score,
            recall_score,
            f1_score,
        )

        df = df.copy()
        df.columns = df.columns.str.strip()

        run_date = pd.to_datetime(df["RunDate"].iloc[0])
        end_date = pd.to_datetime(df["EndDate"], errors="coerce")

        # Encoded labels:
        #   0 = ends within 0-3 months   (imminent)
        #   1 = ends within 3-6 months   (medium)
        #   2 = no end date or >6 months (stable)
        cutoff_3m = run_date + pd.DateOffset(months=3)
        cutoff_6m = run_date + pd.DateOffset(months=6)

        true_cls = np.where(
            end_date.notna() & (end_date >= run_date) & (end_date <= cutoff_3m), 0,
            np.where(
                end_date.notna() & (end_date > cutoff_3m) & (end_date <= cutoff_6m), 1,
                2,
            ),
        )

        pred_col = "Prediction" if "Prediction" in df.columns else "prediction"
        pred_cls = df[pred_col].astype(int).to_numpy()

        # Counts
        from collections import Counter
        pred_counts = Counter(pred_cls.tolist())
        true_counts = Counter(true_cls.tolist())

        logging.info(f"Test run_date: {run_date.date()} | Rows: {len(df)}")
        logging.info(f"Predicted counts: {dict(sorted(pred_counts.items()))}")
        logging.info(f"Actual    counts: {dict(sorted(true_counts.items()))}")

        # Per-class precision / recall / f1
        labels = [0, 1, 2]
        prec = precision_score(true_cls, pred_cls, labels=labels, average=None, zero_division=0)
        rec = recall_score(true_cls, pred_cls, labels=labels, average=None, zero_division=0)
        f1 = f1_score(true_cls, pred_cls, labels=labels, average=None, zero_division=0)

        names = {0: "Imminent (0-3mo)", 1: "Medium (3-6mo)", 2: "Stable (>6mo / none)"}
        for i, lbl in enumerate(labels):
            logging.info(
                f"Class {lbl} [{names[lbl]}] | "
                f"Precision: {prec[i]:.4f} | Recall: {rec[i]:.4f} | F1: {f1[i]:.4f} | "
                f"Predicted: {pred_counts.get(lbl, 0)} | Actual: {true_counts.get(lbl, 0)}"
            )

        logging.info(
            "Confusion Matrix (rows=actual, cols=predicted, order=[0,1,2]):\n"
            + str(confusion_matrix(true_cls, pred_cls, labels=labels))
        )
        logging.info(
            "Classification Report:\n"
            + classification_report(
                true_cls, pred_cls, labels=labels,
                target_names=[names[l] for l in labels], zero_division=0,
            )
        )

        # Save a small JSON next to the CSV for run-over-run comparison.
        import json
        eval_path = DATA_DIR / f"eval_{run_date.date()}.json"
        with open(eval_path, "w") as f:
            json.dump({
                "run_date": str(run_date.date()),
                "rows": int(len(df)),
                "predicted_counts": {int(k): int(v) for k, v in pred_counts.items()},
                "actual_counts": {int(k): int(v) for k, v in true_counts.items()},
                "per_class": {
                    int(lbl): {
                        "name": names[lbl],
                        "precision": float(prec[i]),
                        "recall": float(rec[i]),
                        "f1": float(f1[i]),
                    }
                    for i, lbl in enumerate(labels)
                },
            }, f, indent=2)
        logging.info(f"Eval metrics saved to {eval_path}")

        return df

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
                PredictionConfidence,
                EndDate
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                "PredictionConfidence",
                "EndDate"
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

    evaluated = evaluate_task(final_output)

    upsert = upsert_to_sql(evaluated)

    upsert

    

    