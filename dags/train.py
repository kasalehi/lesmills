from src.les.train.run import snapshot as snapshot_fn
from src.les.train.readdata import read_data as read_data_fn
from src.les.train.ingestfortrain import ingest_data
from src.les.train.train import ModelTraing   # ⚠️ adjust if needed

from airflow import DAG
from airflow.sdk import task
from datetime import datetime
from pathlib import Path

import pandas as pd
import joblib
import json
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import LabelEncoder

# ================================
# PATHS
# ================================
PREPROCESSOR_PATH = "/usr/local/airflow/include/artifacts/preprocessor.pkl"
ARTIFACT_DIR = Path("/usr/local/airflow/include/artifacts")
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)


# ================================
# TRANSFORM + TRAIN
# ================================
def transform_and_train(data_paths: dict):
    try:
        # ✅ Load merged parquet
        df = pd.read_parquet(data_paths["df_merged_path"])

        TARGET = "Churned"

        X = df.drop(TARGET, axis=1)
        

        le = LabelEncoder()
        y = le.fit_transform(df[TARGET])

        # Drop IDs
        for col in ["MembershipID", "member_id"]:
            if col in X.columns:
                X = X.drop(col, axis=1)

        # Split
        x_train, x_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        # Load preprocessor
        preprocessor = joblib.load(PREPROCESSOR_PATH)

        # Train model
        model_obj = ModelTraing()
        trained_model, best_model_name, results = model_obj.trainingModel(
    x_train, y_train, x_test, y_test, preprocessor
)

        # =========================
        # EVALUATION
        # =========================
        y_pred = trained_model.predict(x_test)
        accuracy = accuracy_score(y_test, y_pred)

        print(f"✅ Model Accuracy: {accuracy:.4f}")

        # =========================
        # SAVE ARTIFACTS
        # =========================
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save model
        model_path = ARTIFACT_DIR / f"model_{timestamp}.pkl"
        joblib.dump(trained_model, model_path)

        # Save metrics
        metrics = {
            "accuracy": float(accuracy),
            "rows": len(df),
            "timestamp": timestamp
        }

        metrics_path = ARTIFACT_DIR / f"metrics_{timestamp}.json"
        with open(metrics_path, "w") as f:
            json.dump(metrics, f, indent=4)

        # Save log
        log_path = ARTIFACT_DIR / f"log_{timestamp}.txt"
        with open(log_path, "w") as f:
            f.write(f"Accuracy: {accuracy:.4f}\n")
            f.write(f"Rows: {len(df)}\n")

        print("📦 Artifacts saved:")
        print(model_path)
        print(metrics_path)
        print(log_path)

    except Exception as e:
        raise Exception(f"Training failed: {e}")


# ================================
# DAG
# ================================
with DAG(
    dag_id="ModelTrain",
    description="Les Mills train model pipeline",
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
        return ingest_data(snap_df)   # ✅ returns parquet paths

    # ---------------- TRAIN ----------------
    @task
    def train_task(data_paths):
        transform_and_train(data_paths)

    # ================================
    # FLOW
    # ================================
    raw_df = read_data_task()
    snap_df = snapshots_task(raw_df)
    data_paths = ingest_task(snap_df)

    train_task(data_paths)