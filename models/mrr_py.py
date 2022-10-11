import snowflake.snowpark.functions as f
from snowflake.snowpark.functions import coalesce, concat_ws, lag, md5, col, lit, when, least
from snowflake.snowpark.window import Window

def model(dbt, session):
    dbt.config(materialized='table')

    df_unioned = dbt.ref('customer_revenue_by_month_py').union(dbt.ref('customer_churn_month_py'))

    agg_window = Window.partitionBy("customer_id").orderBy("date_month")
    df_mrr_with_changes = df_unioned.withColumn(
        "previous_month_is_active", coalesce(lag(col("is_active")).over(agg_window), lit(False))).withColumn(
        "previous_month_mrr", coalesce(lag(col("mrr")).over(agg_window), lit(0))).withColumn(
        "mrr_change", (col("mrr") - col("previous_month_mrr")))

    df_final = df_mrr_with_changes.withColumn(
        "id", md5(concat_ws(col("date_month"), col("customer_id")))).withColumn(
        "change_category", when(col("is_first_month"), lit("new")).when(
            (~col("is_active") & col("previous_month_is_active")), lit("churn")).when(
            (col("is_active") & (~col("previous_month_is_active"))), lit("reactivation")).when(
            (col("mrr_change") > 0), lit("upgrade")).when(
            (col("mrr_change") < 0), lit("downgrade"))).withColumn(
        "renewal_amount", least(col("mrr"), col("previous_month_mrr")))

    return df_final