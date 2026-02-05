# Databricks notebook source
# MAGIC %run ./utils_simulation

# COMMAND ----------


def summarize_sales_trend():
    df = spark.table("cpg_sales_gold_monthly")

    first = df.orderBy("year", "month").first()
    last = df.orderBy("year", "month").collect()[-1]

    growth = ((last.total_revenue - first.total_revenue) / first.total_revenue) * 100

    return (
        f"Sales have grown from {first.total_revenue:.2f} to "
        f"{last.total_revenue:.2f} over the period, "
        f"representing a growth of {growth:.1f}%. "
        f"This indicates an overall {'positive' if growth > 0 else 'negative'} trend."
    )

# COMMAND ----------

def summarize_promo_impact():
    df = spark.table("cpg_sales_gold_promo").collect()

    promo = [r for r in df if r.is_promo][0]
    non_promo = [r for r in df if not r.is_promo][0]

    uplift = ((promo.avg_units_sold - non_promo.avg_units_sold) / non_promo.avg_units_sold) * 100

    return (
        f"Promotions increase average units sold by approximately {uplift:.1f}%. "
        f"This shows that promotional campaigns are effective in driving demand."
    )

# COMMAND ----------

def summarize_price_hike():
    df = simulate_price_hike(10, 5)

    avg_revenue = df.selectExpr("avg(sim_revenue)").collect()[0][0]

    return (
        f"A simulated 10% price increase with a 5% demand drop results in "
        f"an average simulated revenue of {avg_revenue:.2f}. "
        f"This suggests that moderate price hikes may still improve revenue."
    )

# COMMAND ----------

def llm_classify_intent(question):
    q = question.lower()

    if any(k in q for k in ["price hike", "increase price", "price increase"]):
        return "PRICE_HIKE"

    if any(k in q for k in ["promo", "promotion"]):
        return "PROMO_ANALYSIS"

    if any(k in q for k in ["trend", "performance", "sales"]):
        return "TREND_ANALYSIS"

    return "UNKNOWN"

# COMMAND ----------

def agent_with_nlp_response(question):
    intent = llm_classify_intent(question)

    if intent == "TREND_ANALYSIS":
        return summarize_sales_trend()

    if intent == "PROMO_ANALYSIS":
        return summarize_promo_impact()

    if intent == "PRICE_HIKE":
        return summarize_price_hike()

    return (
        "I can help with sales trends, promotion impact, "
        "and price hike simulations. Please ask a related question."
    )

# COMMAND ----------

print("=" * 60)
print("SMART CPG DECISION SUPPORT AGENT (NLP UI)")
print("Ask business questions in natural language")
print("Type 'exit' to quit")
print("=" * 60)

while True:
    question = input("\nAsk a business question: ")

    if question.lower() == "exit":
        print("Exiting agent...")
        break

    response = agent_with_nlp_response(question)
    print("\nAgent Insight:")
    print(response)
    print("-" * 60)