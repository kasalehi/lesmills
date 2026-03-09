from pathlib import Path
import os
import pandas as pd
import traceback
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from io import BytesIO, StringIO
def evaluate():
    dates =Variable.get("start_date")
    BUCKET = "lmno_lesmills"
    monthlychurned_prefix='monthlychurned'
    snapshots_prefix="snapshots"
    final_prefix="final"
    try:
        # 1) churn: weekly rows -> one row per member (bucket & churn_flag)
        gcs_hook = GCSHook(gcp_conn_id="gcp_conn")
        churn_path = f"{monthlychurned_prefix}/churned_{dates}.csv"
        
        content=gcs_hook.download(
        bucket_name=BUCKET,
        object_name=churn_path,
        filename=None,
    )
        if isinstance(content, bytes):
            buffer=BytesIO(content)
        else:
            buffer=StringIO(content)
        dfc = pd.read_csv(
            buffer,
            parse_dates=["week"],
            dtype={
                "MembershipID": "string",
                "paused": "int16",
            },
            low_memory=False,
        )



        dfc["end_month_bucket"] = pd.to_numeric(
            dfc["end_month_bucket"], errors="coerce"
        )  # keep NaN

        churn_one = (
            dfc.sort_values(["member_id", "week"])
            .groupby("member_id", as_index=False)
            .agg(
                end_month_bucket=("end_month_bucket", "first"),
                churn_flag=("churn_flag", "max"),
            )
        )




        # 2) snapshots: latest risk_band per member
        snap_path = f"{snapshots_prefix}/snapshot_{dates}.csv"
        content_snap=gcs_hook.download(
        bucket_name=BUCKET,
        object_name=snap_path,
        filename=None, 
        )
        if isinstance(content_snap, bytes):
            buffer_snap=BytesIO(content_snap)
        else:
            buffer_snap=StringIO(content_snap)

        dfs = pd.read_csv(
            buffer_snap, 
            parse_dates=["week"]
            
            )
        snap_latest = (
            dfs.sort_values(["member_id", "week"])
            .drop_duplicates(subset=["member_id"], keep="last")[
                ["member_id", "risk_band"]
            ]
        )
        snap_latest["risk_band"] = (
            snap_latest["risk_band"]
            .astype(str)
            .str.strip()
            .str.capitalize()
            .replace({"Lowe": "Low", "Meduim": "Medium"})
        )

        # 3) LEFT JOIN snapshots → churn
        merged = snap_latest.merge(churn_one, on="member_id", how="left")
        merged["churn_flag"] = merged["churn_flag"].fillna(0).astype(int)

        order_rows = ["Low", "Medium", "High", "Critical"]
        month_cols = list(range(1, 13))

        # 4) counts by month bucket (1..12)
        counts = (
            merged[merged["end_month_bucket"].isin(month_cols)]
            .pivot_table(
                index="risk_band",
                columns="end_month_bucket",
                values="member_id",
                aggfunc="nunique",
                fill_value=0,
            )
            .reindex(index=order_rows, fill_value=0)
            .reindex(columns=month_cols, fill_value=0)
        )

        # 5) null bucket (no end within 12m or not in churn)
        null_counts = (
            merged[merged["end_month_bucket"].isna()]
            .groupby("risk_band")["member_id"]
            .nunique()
            .reindex(order_rows, fill_value=0)
            .rename("null")
        )

        # 6) totals per risk band (universe = snapshots)
        total_members = (
            merged.groupby("risk_band")["member_id"]
            .nunique()
            .reindex(order_rows, fill_value=0)
        )
        total_churned = counts.sum(axis=1)
        total_not_churned = total_members - total_churned

        totals_df = pd.DataFrame(
            {
                "Total_Members": total_members.astype(int),
                "Total_Churned": total_churned.astype(int),
                "Total_Not_Churned": total_not_churned.astype(int),
                "Total_Churned_And_Not": total_members.astype(int),  # = Total_Members
            }
        )
        totals_df["%_Churned"] = (
            totals_df["Total_Churned"] / totals_df["Total_Members"]
        ).fillna(0)

        # 7) per-month % columns using Total_Members denominator
        pct = counts.div(total_members.replace(0, pd.NA), axis=0).fillna(0)
        pct = pct.rename(
            columns={
                m: (f"%churned_{m}month" if m == 1 else f"%churned_{m}months")
                for m in month_cols
            }
        )

        # 8) assemble final
        final = pd.concat([counts, null_counts, pct, totals_df], axis=1)

        # round % columns
        pct_cols = [
            c
            for c in final.columns
            if isinstance(c, str) and c.startswith("%churned_")
        ] + ["%_Churned"]
        final[pct_cols] = final[pct_cols].round(4)


        final_path= f"{final_prefix} / final_{dates}.csv"

        gcs_hook.upload(
        bucket_name=BUCKET,
        object_name=final_path,     
        data=final.to_csv(index=False),
        mime_type="text/csv",
    )

    except Exception as e:
        print(
            f"❌ Error occures "
        )
        traceback.print_exc()
