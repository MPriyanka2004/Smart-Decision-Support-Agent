# Databricks notebook source
# MAGIC %run ../utils_agents

# COMMAND ----------

response = agent_with_nlp_response("How are sales performing?")
assert isinstance(response, str)
assert len(response) > 20

response = agent_with_nlp_response("Do promotions help sales?")
assert "promo" in response.lower() or "promotion" in response.lower()

response = agent_with_nlp_response("What happens if we increase prices?")
assert "price" in response.lower()

print("Agent tests passed")