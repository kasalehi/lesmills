import sys
import pandas as pd
from src.les.exception import CustomException
from src.les.logger import logger
from src.les.transform import Transform
from src.les.train import ModelTraing
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
def ingest_data(snap: pd.DataFrame) -> pd.DataFrame:
    try:
        start_date = Variable.get("start_date")   
        hook = MsSqlHook(mssql_conn_id="lesmills_mssql")
        query = f"""
        declare @TestDate as date;
        set @TestDate='{start_date}';
        WITH base AS (
                SELECT
                    cm.MembershipID,
                    cm.[End Date] ,
                    cm.[Status Desc],
                    cm.SubCategory,
                    cm.RegularPayment,
                    cm.Gender,
                    cm.MembershipTypeDesc ,
                    -- Age: no need to ROUND, DATEDIFF already returns INT
                    DATEDIFF(YEAR, cm.DOB, GETDATE()) AS Age,
                    -- Churn flag
                CASE
                    WHEN cm.[End Date] <= DATEADD(MONTH,  3, @TestDate)  and cm.[End Date] >=@TestDate THEN  1
                    WHEN cm.[End Date] <= DATEADD(MONTH,  6, @TestDate) and cm.[End Date] >=@TestDate  THEN 2
                    ELSE 3
                END AS Churned,
                    att.WeekVisits
                FROM fact.LMNZ_ALLMemberships AS cm 
                JOIN repo.MemberWeeklyAttendanceCounts AS att
                    ON cm.MembershipID = att.MembershipID
                    where cm.[Start Date]<@TestDate and (cm.[End Date]>@TestDate or cm.[End Date] is null) 
            ),
            total as (
            SELECT
                MembershipID,
                SubCategory,
                RegularPayment,
                Gender,
                Age,
                SUM(WeekVisits) AS TotalAttendance
            FROM base
            GROUP BY
                MembershipID ,
                [Status Desc],
                SubCategory,
                RegularPayment,
                Gender,
                Age)
                select * from total where SubCategory NOT IN ('Unvaccinated', 'Prepay')
            """
        df=hook.get_pandas_df(sql=query) 
        snaps = snap[['member_id','ews_pct','risk_band']]
        df['member_id'] = df["MembershipID"].astype(str).str.strip().str.lower()
        snaps['member_id'] = snaps['member_id'].astype(str).str.strip().str.lower()
        logger.info(f"SQL rows: {len(df)}")
        logger.info(f"Snap rows: {len(snaps)}")
        merged = df.merge(snaps, how='inner', on='member_id')
        logger.info(f"Merged rows: {len(merged)}")
        return merged
    except Exception as e:
        raise CustomException(e, sys)
def transform_data(merged: pd.DataFrame):
    try:
        trans = Transform()
        x_train, y_train, x_test, y_test = trans.splitting_data(merged)
        preprocessor = trans.preprocessor()
        return x_train, y_train, x_test, y_test, preprocessor
    except Exception as e:
        raise CustomException(e, sys)


def train_model(x_train, y_train, x_test, y_test, preprocessor):
    try:
        model = ModelTraing()
        model.trainingModel(x_train, y_train, x_test, y_test, preprocessor)
    except Exception as e:
        raise CustomException(e, sys)