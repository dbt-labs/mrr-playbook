import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import as_double, col, dateadd, lit, to_date

def model(dbt, session):
    dbt.config(materialized='table')

    df_mrr = dbt.ref('customer_revenue_by_month_py')

    df = df_mrr.filter(col("is_last_month")).select(
        dateadd('month', lit(1), to_date(col("date_month"))).alias("date_month"),
        col("customer_id"),
        as_double(lit(0)).alias("mrr"),
        lit(False).alias("is_active"),
        col("first_active_month"),
        col("last_active_month"),
        lit(False).alias("is_first_month"),
        lit(False).alias("is_last_month"))

    return df