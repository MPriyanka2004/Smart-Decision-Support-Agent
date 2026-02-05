# Databricks notebook source
silver_df = spark.table("cpg_sales_silver")

# COMMAND ----------

# MAGIC %run ./utils_simulation

# COMMAND ----------

display(simulate_price_hike(10, 5))

# COMMAND ----------

from pyspark.sql.functions import col

def simulate_promo(discount_pct, uplift_pct):
    return (
        silver_df
        .withColumn("sim_price", col("price") * (1 - discount_pct/100))
        .withColumn("sim_units", col("units_sold") * (1 + uplift_pct/100))
        .withColumn("sim_revenue", col("sim_price") * col("sim_units"))
    )

display(simulate_promo(20, 30))

# COMMAND ----------

def simulate_supply_shortage(stock_drop_pct):
    return (
        silver_df
        .withColumn(
            "sim_units",
            col("units_sold") * (1 - stock_drop_pct/100)
        )
        .withColumn("sim_revenue", col("sim_units") * col("price"))
    )

display(simulate_supply_shortage(30))