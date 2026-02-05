# Databricks notebook source
# MAGIC %run ../utils_simulation

# COMMAND ----------

df = spark.table("cpg_sales_gold_monthly")
assert "total_revenue" in df.columns
assert df.count() > 0

df = spark.table("cpg_sales_gold_promo")
assert df.count() == 2

df = simulate_price_hike(10, 5)
assert "sim_revenue" in df.columns
assert df.count() > 0

print("Tool tests passed")