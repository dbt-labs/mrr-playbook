import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import date_trunc, min, max, col, iff, lit, when
from snowflake.snowpark.window import Window

def model(dbt, session):
    dbt.config(materialized='table')

    df_periods = dbt.ref('subscription_periods')
    df_months = dbt.ref('util_months')

    # determine when a given account had its first and last (or most recent) month
    df_customers = df_periods.group_by(col('customer_id'
            )).agg([min(date_trunc('MONTH', 'start_date')).alias('date_month_start'),
                   max(date_trunc('MONTH', 'end_date')).alias('date_month_end')])


    # create one record per month between a customer's first and last month
    # (example of a date spine)
    df_cust_months = df_customers.join(df_months,
            (df_months.col('date_month') >= df_customers.col('date_month_start')) # all months after start date
            & (df_months.col('date_month') < df_customers.col('date_month_end'    # and before end date
            ))).select(df_customers.col('customer_id').alias('customer_id1'),
                      df_months.col('date_month'))

    # join the account-month spine to MRR base model, pulling through most recent dates
    # and plan info for month rows that have no invoices (i.e. churns) 
    df_joined = df_cust_months.join(df_periods,
            (df_cust_months.col("customer_id1") == df_periods.col("customer_id"))
            & (df_cust_months.col("date_month") >= df_periods.col("start_date")) # month is after a subscription start date
            & ((df_cust_months.col("date_month") < df_periods.col("end_date"))   # month is before a subscription end date
                | (df_periods.col("end_date").isNull())),                        # (and handle null case)
            "left"
            ).select(df_cust_months.col("date_month"),
                    df_cust_months.col("customer_id1").alias("customer_id"),
                    f.coalesce(df_periods.col("monthly_amount"), lit(0)).alias("mrr"))

    cust_id_window = Window.partitionBy("customer_id")
    df = df_joined.select(col("date_month"), 
        col("customer_id"),
        col("mrr"),
        (col("mrr") > lit(0)).alias("is_active"),
        # calculate first and last months
        min(when(col("is_active"), col("date_month"))).over(cust_id_window).alias("first_active_month"),
        max(when(col("is_active"), col("date_month"))).over(cust_id_window).alias("last_active_month"),
        # calculate if this record is the first or last month
        (col("first_active_month") == col("date_month")).alias("is_first_month"),
        (col("last_active_month") == col("date_month")).alias("is_last_month"))

    return df