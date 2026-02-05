# Databricks notebook source
silver_df = spark.table("cpg_sales_silver")
display(silver_df)

# COMMAND ----------

display(spark.table("cpg_sales_gold_monthly"))

# COMMAND ----------

from pyspark.sql.functions import avg

seasonality = (
    silver_df
    .groupBy("month")
    .agg(avg("revenue").alias("avg_monthly_revenue"))
    .orderBy("month")
)

display(seasonality)

# COMMAND ----------

from pyspark.sql.functions import col, avg

avg_revenue = silver_df.select(
    avg("revenue").alias("avg_rev")
).collect()[0]["avg_rev"]

anomalies = silver_df.filter(col("revenue") > avg_revenue * 2)

display(anomalies)