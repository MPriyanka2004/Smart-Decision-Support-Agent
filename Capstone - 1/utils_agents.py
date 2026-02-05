# Databricks notebook source
# MAGIC %run ./utils_simulation

# COMMAND ----------

def llm_classify_intent(question: str) -> str:
    """
    Classifies user intent from a natural language question.
    Rule-based NLP (LLM-ready design).
    """
    q = question.lower()

    if any(k in q for k in ["price hike", "increase price", "price increase", "raise price"]):
        return "PRICE_HIKE"

    if any(k in q for k in ["promo", "promotion", "discount"]):
        return "PROMO_ANALYSIS"

    if any(k in q for k in ["trend", "performance", "sales", "growth"]):
        return "TREND_ANALYSIS"

    if any(k in q for k in ["what is", "define"]):
        return "DEFINITION"

    return "UNKNOWN"

# COMMAND ----------

# MAGIC %md
# MAGIC ### NLP Summaries

# COMMAND ----------

def summarize_sales_trend() -> str:
    """
    Returns a natural-language summary of sales trends.
    """
    df = spark.table("cpg_sales_gold_monthly")

    first = df.orderBy("year", "month").first()
    last = df.orderBy("year", "month").collect()[-1]

    growth_pct = (
        (last.total_revenue - first.total_revenue) / first.total_revenue
    ) * 100

    direction = "positive" if growth_pct > 0 else "negative"

    return (
        f"Sales show a {direction} trend, growing by "
        f"{growth_pct:.1f}% over the observed period. "
        f"This indicates overall business performance is improving."
    )


def summarize_promo_impact() -> str:
    """
    Returns a natural-language summary of promotion impact.
    """
    df = spark.table("cpg_sales_gold_promo").collect()

    promo = [r for r in df if r.is_promo][0]
    non_promo = [r for r in df if not r.is_promo][0]

    uplift_pct = (
        (promo.avg_units_sold - non_promo.avg_units_sold)
        / non_promo.avg_units_sold
    ) * 100

    return (
        f"Promotions increase average units sold by approximately "
        f"{uplift_pct:.1f}%, demonstrating that promotional campaigns "
        f"are effective in driving higher demand."
    )


def summarize_price_hike(hike_pct: int = 10, demand_drop_pct: int = 5) -> str:
    """
    Returns a natural-language summary of a price hike simulation.
    """
    df = simulate_price_hike(hike_pct, demand_drop_pct)

    avg_revenue = df.selectExpr("avg(sim_revenue)").collect()[0][0]

    return (
        f"A simulated {hike_pct}% price increase with a "
        f"{demand_drop_pct}% demand drop results in an average "
        f"simulated revenue of {avg_revenue:.2f}. "
        f"This suggests that moderate price hikes may still "
        f"improve overall revenue."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agent Orchestration

# COMMAND ----------

def agent_with_nlp_response(question: str) -> str:
    """
    Main agent entry point.
    Takes a natural-language question and returns a business insight.
    """
    intent = llm_classify_intent(question)

    if intent == "DEFINITION" and "price hike" in question.lower():
        return (
            "A price hike refers to an increase in the selling price of a product. "
            "Businesses typically apply price hikes to improve margins or offset "
            "rising costs, but they may reduce demand if customers are price-sensitive."
        )

    if intent == "TREND_ANALYSIS":
        return summarize_sales_trend()

    if intent == "PROMO_ANALYSIS":
        return summarize_promo_impact()

    if intent == "PRICE_HIKE":
        return summarize_price_hike()
    
    return (
        "I can help with sales trends, promotion impact, and price hike simulations. "
        "Please ask a related business question."
    )