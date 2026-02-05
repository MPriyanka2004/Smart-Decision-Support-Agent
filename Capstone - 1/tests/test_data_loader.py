# Databricks notebook source
def test_bronze_table_exists(spark):
    df = spark.table("cpg_sales_table")
    assert df.count() > 0, "Bronze table is empty"
    print("Bronze table test passed")


def test_silver_table_exists(spark):
    df = spark.table("cpg_sales_silver")
    assert df.count() > 0, "Silver table is empty"
    print("Silver table test passed")


def test_gold_tables_exist(spark):
    spark.table("cpg_sales_gold_monthly")
    spark.table("cpg_sales_gold_promo")
    spark.table("cpg_sales_gold_region_category")
    print("Gold tables test passed")

# COMMAND ----------

test_bronze_table_exists(spark)
test_silver_table_exists(spark)
test_gold_tables_exist(spark)

print("ALL TESTS PASSED SUCCESSFULLY")
