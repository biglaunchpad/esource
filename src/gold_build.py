# Databricks notebook source
# Gold: current state of each Silver entity (current rows, SCD columns dropped).
# It's just a projection, so we rebuild it each run rather than create it in setup.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
CAT = dbutils.widgets.get("catalog")
SILVER, GOLD = f"{CAT}.silver", f"{CAT}.gold"
SCD = ["effective_from", "effective_to", "is_current", "row_hash"]

# COMMAND ----------
for t in ["circuit", "installed_der", "planned_der"]:
    (spark.table(f"{SILVER}.{t}").filter("is_current = true").drop(*SCD)
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.{t}"))
print("gold:", {t: spark.table(f"{GOLD}.{t}").count() for t in ["circuit", "installed_der", "planned_der"]})
print("\nGold build complete.")
