# Databricks notebook source
from pyspark.sql.functions import col

def simulate_price_hike(hike_pct, demand_drop_pct):
    df = spark.table("cpg_sales_silver")
    return (
        df
        .withColumn("sim_price", col("price") * (1 + hike_pct / 100))
        .withColumn("sim_units", col("units_sold") * (1 - demand_drop_pct / 100))
        .withColumn("sim_revenue", col("sim_price") * col("sim_units"))
    )