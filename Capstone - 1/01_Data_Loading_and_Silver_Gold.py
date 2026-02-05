# Databricks notebook source
import pandas as pd
import numpy as np

np.random.seed(123)

# Parameters
num_rows = 5000
date_range = pd.date_range('2022-01-01', periods=365)
stores = list(range(1, 11))
regions = ['North', 'South', 'East']
skus = list(range(101, 151))
categories = ['Beverages', 'Snacks', 'Dairy', 'Household', 'Personal Care']
promo_types = [None, 'Discount', 'BuyOneGetOne', 'FlashSale']
store_sizes = ['Small', 'Medium', 'Large']

data = []

for _ in range(num_rows):
    date = pd.to_datetime(np.random.choice(date_range))

    store = np.random.choice(stores)
    store_region = np.random.choice(regions)
    sku = np.random.choice(skus)
    category = np.random.choice(categories)

    base_price = np.round(np.random.uniform(2, 10), 2)

    promo_flag = np.random.choice([0, 1], p=[0.8, 0.2])
    promo_type = np.random.choice(promo_types) if promo_flag else None

    price = base_price * (0.8 if promo_flag else 1.0)
    units_sold = np.random.poisson(20) if promo_flag else np.random.poisson(10)
    revenue = units_sold * price

    inventory_level = np.random.randint(100, 1000)
    store_size = np.random.choice(store_sizes)

    # Weekend = Holiday
    holiday_flag = 1 if date.weekday() in [5, 6] else 0

    data.append([
        date, store, store_region, sku, category,
        units_sold, revenue, promo_flag, promo_type,
        price, inventory_level, store_size, holiday_flag
    ])

df = pd.DataFrame(data, columns=[
    'date', 'store_id', 'store_region', 'sku_id', 'category',
    'units_sold', 'revenue', 'promo_flag', 'promo_type',
    'price', 'inventory_level', 'store_size', 'holiday_flag'
])

df.to_csv('cpg_sales_sample_5000.csv', index=False)

print("CSV with 5000 rows created successfully.")


# COMMAND ----------

display(df)

# COMMAND ----------

type(df)

# COMMAND ----------

spark_df = spark.createDataFrame(df)
display(spark_df)

# COMMAND ----------

spark_df.write \
    .mode("overwrite") \
    .saveAsTable("cpg_sales_table")

# COMMAND ----------

bronze_df = spark.table("cpg_sales_table")
display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data volume 

# COMMAND ----------

bronze_df.selectExpr(
    "count(*) as total_rows",
    "count(distinct date) as total_days",
    "count(distinct store_id) as total_stores",
    "count(distinct sku_id) as total_skus"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema 

# COMMAND ----------

bronze_df.printSchema()
bronze_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date range

# COMMAND ----------

bronze_df.selectExpr(
    "min(date) as start_date",
    "max(date) as end_date"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing values check

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum

bronze_df.select([
    _sum(col(c).isNull().cast("int")).alias(c)
    for c in bronze_df.columns
]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribution analysis

# COMMAND ----------

bronze_df.select("units_sold", "revenue", "price").summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Region-wise performance

# COMMAND ----------

bronze_df.groupBy("store_region") \
    .sum("revenue", "units_sold") \
    .orderBy("sum(revenue)", ascending=False) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Category - Wise Performance

# COMMAND ----------

bronze_df.groupBy("category") \
    .sum("revenue", "units_sold") \
    .orderBy("sum(revenue)", ascending=False) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Promo vs Non - Promo

# COMMAND ----------

bronze_df.groupBy("promo_flag") \
    .avg("units_sold", "revenue") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store Size Impact

# COMMAND ----------

bronze_df.groupBy("store_size") \
    .avg("units_sold", "revenue") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Holiday Impact

# COMMAND ----------

bronze_df.groupBy("holiday_flag") \
    .avg("units_sold", "revenue") \
    .show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver layer

# COMMAND ----------

silver_df = (
    bronze_df
    .withColumn("date", col("date").cast("date"))
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .withColumn(
        "revenue_per_unit",
        col("revenue") / col("units_sold")
    )
    .withColumn(
        "is_promo",
        when(col("promo_flag") == 1, True).otherwise(False)
    )
)

display(silver_df)

# COMMAND ----------

silver_df.write \
    .mode("overwrite") \
    .saveAsTable("cpg_sales_silver")

# COMMAND ----------

display(spark.table("cpg_sales_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold layer

# COMMAND ----------

gold_monthly = (
    spark.table("cpg_sales_silver")
    .groupBy("year", "month")
    .sum("revenue", "units_sold")
    .withColumnRenamed("sum(revenue)", "total_revenue")
    .withColumnRenamed("sum(units_sold)", "total_units")
)

display(gold_monthly)

# COMMAND ----------

gold_monthly.write \
    .mode("overwrite") \
    .saveAsTable("cpg_sales_gold_monthly")

# COMMAND ----------

from pyspark.sql.functions import avg

gold_promo = (
    spark.table("cpg_sales_silver")
    .groupBy("is_promo")
    .agg(
        avg("units_sold").alias("avg_units_sold"),
        avg("revenue").alias("avg_revenue")
    )
)

display(gold_promo)


# COMMAND ----------

gold_promo.write \
    .mode("overwrite") \
    .saveAsTable("cpg_sales_gold_promo")

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum

gold_region_category = (
    spark.table("cpg_sales_silver")
    .groupBy("store_region", "category")
    .agg(
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("units_sold").alias("total_units_sold")
    )
)

display(gold_region_category)


# COMMAND ----------

gold_region_category.write \
    .mode("overwrite") \
    .saveAsTable("cpg_sales_gold_region_category")

# COMMAND ----------

# MAGIC %md
# MAGIC