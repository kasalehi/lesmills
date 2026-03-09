from pathlib import Path
import pandas as pd
<<<<<<< HEAD
from include.lesmills_project.app import build_snapshots, score_catboost, build_outreach
from airflow.models import Variable
from include.lesmills_project.ingest import read_data
def snapshot(df: pd.DataFrame) -> pd.DataFrame:
=======
from include.lesmills_project.app import *
from airflow.models import Variable
import os
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from io import BytesIO, StringIO

def snapshot():
    BUCKET = "lmno_lesmills"
    datasource_prefix = "datasources"
    snapshots_prefix="snapshots"
    date_str = Variable.get("start_date")
    snap_path = f"{snapshots_prefix}/snapshot_{date_str}.csv"
    gcs_hook = GCSHook(gcp_conn_id="gcp_conn")
    object_path = f"{datasource_prefix}/datasource_{date_str}.csv"
    content=gcs_hook.download(
        bucket_name=BUCKET,
        object_name=object_path,
        filename=None,
    )
    # lets read the data as BytesIO, StringIO 
    if isinstance(content, bytes):
        buffer=BytesIO(content)
    else:
        buffer=StringIO(content)
    
    df = pd.read_csv(
        buffer,
        parse_dates=["week"],
        dtype={
            "MembershipID": "string",
            "paused": "int16",
        },
        low_memory=False,
    )

>>>>>>> 34f3a5f24a0abcc543b1c34e6fe2aca904298887
    active_q, paused_q, scores, model, feature_cols = build_outreach(
        df,
        k_weeks=6,
        capacity=500,
    )
    scores = score_catboost(model, feature_cols, k_weeks=6, df=df)
    snapshots = build_snapshots(scores, df)

    snapshots["main_reason"] = snapshots["main_reason"].replace({
        "erratic_usage": "Irregular_Movement",
        "drought_streak": "Continously_Inactive",
    })
    snapshots["reasons"] = snapshots["reasons"].replace({
        "erratic_usage": "Irregular_Movement",
        "drought_streak": "Continously_Inactive",
    })

<<<<<<< HEAD
    return snapshots
=======
    gcs_hook.upload(
        bucket_name=BUCKET,
        object_name=snap_path,     # e.g. "retention/snapshots/final_2025-03-25.csv"
        data=snapshots.to_csv(index=False),
        mime_type="text/csv",
    )
>>>>>>> 34f3a5f24a0abcc543b1c34e6fe2aca904298887
