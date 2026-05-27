import numpy as np
import pandas as pd
import joblib
from pathlib import Path

ARTIFACT_DIR = Path("/usr/local/airflow/include/artifacts")

ID_COL = "MembershipID"

FEATURE_COLS = [
    "SubCategory",
    "RegularPayment",
    "Gender",
    "Age",
    "TotalAttendance",
    "ews_pct",
    "risk_band",
]

# Encoded class 0 = original Churned=1 (imminent, 0-3 months).
# Recall-first threshold override: predict imminent whenever P(imminent) >= IMMINENT_THRESHOLD,
# even if argmax would pick a different class. Lower = more recall, less precision.
IMMINENT_CLASS = 0
IMMINENT_THRESHOLD = 0.50


def load_pipeline():
    model_path = ARTIFACT_DIR / "model.pkl"
    pipline = joblib.load(model_path)
    return pipline


def run_prediction(df: pd.DataFrame):

    pipeline = load_pipeline()

    X = df[FEATURE_COLS]

    result = df[[ID_COL]].copy()

    if hasattr(pipeline, "predict_proba"):
        proba = pipeline.predict_proba(X)
        argmax_pred = proba.argmax(axis=1)
        if IMMINENT_CLASS < proba.shape[1]:
            y_pred = np.where(
                proba[:, IMMINENT_CLASS] >= IMMINENT_THRESHOLD,
                IMMINENT_CLASS,
                argmax_pred,
            )
        else:
            y_pred = argmax_pred
        result["prediction"] = y_pred
        result["confidence"] = proba.max(axis=1)
    else:
        result["prediction"] = pipeline.predict(X)

    return result