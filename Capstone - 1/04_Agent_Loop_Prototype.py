# Databricks notebook source
# MAGIC %run ./utils_simulation

# COMMAND ----------

display(simulate_price_hike(10, 5))

# COMMAND ----------

def agent(question: str):
    q = question.lower()

    if "trend" in q:
        display(spark.table("cpg_sales_gold_monthly"))
        return "Displayed monthly sales trend."

    if "promo" in q:
        display(spark.table("cpg_sales_gold_promo"))
        return "Displayed promo impact analysis."

    if "region" in q:
        display(spark.table("cpg_sales_gold_region_category"))
        return "Displayed region and category performance."

    if "price hike" in q:
        display(simulate_price_hike(10, 5))
        return "Simulated 10% price hike."

    return "Ask about trends, promos, regions, or simulations."

# COMMAND ----------

agent("show sales trend")
agent("promo impact")
agent("compare regions")
agent("simulate price hike")

# COMMAND ----------

conversation_memory = []

def agent_with_memory(question):
    response = agent(question)
    conversation_memory.append((question, response))
    return response

agent_with_memory("show trend")
agent_with_memory("promo impact")
conversation_memory
