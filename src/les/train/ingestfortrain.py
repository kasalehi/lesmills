import sys
import pandas as pd
from src.les.exception import CustomException
from src.les.logger import logger
from airflow.models import Variable
from pathlib import Path
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


def ingest_data(snap: pd.DataFrame):
    start_date = pd.to_datetime(Variable.get("train_date")).strftime("%Y-%m-%d")
    # start_date = pd.Timestamp.today().date()
    try:
        logger.info(f"Start date used in SQL: {start_date}")
        hook = MsSqlHook(mssql_conn_id="lesmills_mssql")
        query =f""" 
        DECLARE @TestDate DATE;
        set @TestDate='{start_date}';
        WITH base AS (
            SELECT
                cm.MembershipID,
                cm.[End Date],
                cm.[Status Desc],
                cm.SubCategory,
                cm.RegularPayment,
                cm.Gender,
                cm.MembershipTypeDesc,
                DATEDIFF(YEAR, cm.DOB, GETDATE()) AS Age,

                CASE
                 WHEN cm.[End Date] BETWEEN @TestDate AND DATEADD(MONTH,3,@TestDate) THEN 1

                 WHEN cm.[End Date] > DATEADD(MONTH,3,@TestDate)
                AND cm.[End Date] <= DATEADD(MONTH,6,@TestDate) THEN 2

                 ELSE 3
                  END AS Churned,

                att.WeekVisits

            FROM fact.LMNZ_ALLMemberships cm
            JOIN repo.MemberWeeklyAttendanceCounts att
            ON cm.MembershipID = att.MembershipID

            WHERE cm.[Start Date] < @TestDate
            AND (cm.[End Date] > @TestDate OR cm.[End Date] IS NULL)
        ),

        total AS (
            SELECT
                MembershipID,
                SubCategory,
                RegularPayment,
                Gender,
                Age,
                Churned,
                SUM(WeekVisits) AS TotalAttendance
            FROM base
            GROUP BY
                MembershipID,
                SubCategory,
                RegularPayment,
                Gender,
                Age, 
                Churned  
        )

        SELECT *
        FROM total
        WHERE SubCategory NOT IN ('Unvaccinated','Prepay', 'Complimentary')
        """

        df = hook.get_pandas_df(sql=query)
        logger.info(query)
        df["MembershipID"] = df["MembershipID"].map(str)
        snaps = snap[['member_id','ews_pct','risk_band']].copy()
        df["member_id"] = df["MembershipID"].str.strip().str.lower()
        snaps["member_id"] = snaps["member_id"].astype(str).str.strip().str.lower()
        logger.info(f"SQL rows: {len(df)}")
        logger.info(f"Snap rows: {len(snaps)}")
        merged = df.merge(snaps, how="inner", on="member_id")
        logger.info(f"Merged rows: {len(merged)}")
        DATA_DIR = Path("/usr/local/airflow/include/data")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        sql_path = DATA_DIR / "df_sql.parquet"
        merged_path = DATA_DIR / "df_merged.parquet"
        df.to_parquet(sql_path, index=False)
        merged.to_parquet(merged_path, index=False)
        return {
            "df_sql_path": str(sql_path),
            "df_merged_path": str(merged_path)
        }

    except Exception as e:
        raise CustomException(e, sys)