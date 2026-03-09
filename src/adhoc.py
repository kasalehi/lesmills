class Adhoc:
    def read(self): 
        from sqlalchemy import create_engine
        import pandas as pd
        from urllib.parse import quote_plus
        import pandas as pd
        server   = r'LMNZLREPORT01\LM_RPT'                 
        database = 'LMNZ_Report'
        driver   = 'ODBC Driver 17 for SQL Server'         
        username = 'LMNZ_ReportUser'
        password = 'LMNZ_ReportUser'                           
        engine = create_engine(
            f"mssql+pyodbc://{username}:{quote_plus(password)}@{server}/{database}"
            f"?driver={driver.replace(' ', '+')}&Encrypt=yes&TrustServerCertificate=yes"
        )
        query = """
        declare @TestDate as date;
        set @TestDate='2025-07-07';
        WITH base AS (
                    SELECT
                        cm.MembershipID,
                        cm.[End Date] ,
                        cm.[Status Desc],
                        cm.SubCategory,
                        cm.Term,
                        cm.PaymentFrequency,
                        cm.RegularPayment,
                        cm.Gender,
                    
				
                        -- Age: no need to ROUND, DATEDIFF already returns INT
                        DATEDIFF(YEAR, cm.DOB, GETDATE()) AS Age,
                        -- Churn flag
                     CASE
                        WHEN cm.[End Date] <= DATEADD(MONTH,  1, @TestDate)  and cm.[End Date] >=@TestDate THEN  1
                        WHEN cm.[End Date] <= DATEADD(MONTH,  2, @TestDate) and cm.[End Date] >=@TestDate  THEN 2
                        WHEN cm.[End Date] <= DATEADD(MONTH, 3, @TestDate) and cm.[End Date] >=@TestDate  THEN  3
						WHEN cm.[End Date] <= DATEADD(MONTH,  4, @TestDate)  and cm.[End Date] >=@TestDate THEN  4
                        WHEN cm.[End Date] <= DATEADD(MONTH,  5, @TestDate) and cm.[End Date] >=@TestDate  THEN 5
                        WHEN cm.[End Date] <= DATEADD(MONTH, 6, @TestDate) and cm.[End Date] >=@TestDate  THEN  6
						WHEN cm.[End Date] <= DATEADD(MONTH,  7, @TestDate)  and cm.[End Date] >=@TestDate THEN  7
                        WHEN cm.[End Date] <= DATEADD(MONTH,  8, @TestDate) and cm.[End Date] >=@TestDate  THEN 8
                        WHEN cm.[End Date] <= DATEADD(MONTH, 9, @TestDate) and cm.[End Date] >=@TestDate  THEN  9
						WHEN cm.[End Date] <= DATEADD(MONTH,  10, @TestDate)  and cm.[End Date] >=@TestDate THEN  10
                        WHEN cm.[End Date] <= DATEADD(MONTH,  11, @TestDate) and cm.[End Date] >=@TestDate  THEN 11
                        WHEN cm.[End Date] <= DATEADD(MONTH, 12, @TestDate) and cm.[End Date] >=@TestDate  THEN  12
                        ELSE 13
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
                    SUM(WeekVisits) AS TotalAttendance,
			        
                    Churned
                FROM base
                GROUP BY
                    MembershipID ,
                    [Status Desc],
                    SubCategory,
                   
                    RegularPayment,
                    Gender,
                    Age,
			       
                    Churned)
                    select * from total where SubCategory NOT IN ('Unvaccinated', 'Prepay')  ;
                """

        df = pd.read_sql_query(query, engine)
        return df
    def merge(self,df):
        def classify_risk(x):
            if x < 0.70:
                return "Low"
            elif x < 0.80:
                return "Medium"
            elif x < 0.90:
                return "High"
            else:
                return "Critical"
        import pandas as pd
        import numpy as np
        predictions=pd.read_csv('datasets\predictions.csv')
        # predictions['risk_category'] = predictions['prob_class_1'].apply(classify_risk)
        self.df=df
        #for merging current data with predictions
        merged=df.merge(predictions, how="inner", on="MembershipID")
        #-----------> #if data is for passed date, it should use left mergin 
        #-----------> # merged=df.merge(predictions, how="inner", on="MembershipID")
        merged_filtered=merged.drop_duplicates("MembershipID")

        merged_filtered.to_csv('datasets/merged20240305.csv')

        churn_counts = (
            merged_filtered.groupby('Churned')
            .size()
            .reindex([1, 2, 3], fill_value=0)
        )

        # 2) correct predictions per class: Churned == prediction == k
        correct_counts = (
        merged_filtered[merged_filtered['Churned'] == merged_filtered['prediction(ChurnedOrNot)']]
        .groupby('Churned')
        .size()
        .reindex([1, 2, 3], fill_value=0)
        )

        # 3) percentage = correct / total churn for that class
        pct = correct_counts / churn_counts.replace(0, np.nan)

        # 4) final table
        final = pd.DataFrame({
            "Month_Category": ["Less Than 3 Months", "Less Than 6 Months", "Others"],
            "Actual_churn_count": churn_counts.values, 
            "Prediction_Churned_count": correct_counts.values,  # Churned==k & prediction==k
            "Accuracy_Percentage": np.round(pct.values,2)                          # recall per class
        })

        final.to_csv('datasets/final20240305.csv', index=False)
obj=Adhoc()
result=obj.read()
obj.merge(result)
