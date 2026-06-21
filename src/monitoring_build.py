# Databricks notebook source
# Monitoring: row counts, missing-capacity, link rate, and freshness per entity from
# Silver. Reads Silver only, so it runs in parallel with gold/platinum. Freshness is
# the load timestamp (effective_from), which is populated for every entity.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
CAT = dbutils.widgets.get("catalog")
SILVER, MONITOR = f"{CAT}.silver", f"{CAT}.monitoring"

# COMMAND ----------
def dq(entity):
    df = spark.table(f"{SILVER}.{entity}").filter("is_current = true")
    cap = "max_hosting_capacity_mw" if entity == "circuit" else "nameplate_rating_mw"
    link = [F.sum(F.col("has_valid_circuit_link").cast("int")).alias("valid_link_rows")] if entity != "circuit" \
        else [F.lit(None).cast("long").alias("valid_link_rows")]
    return (df.groupBy("source_utility").agg(
                F.count("*").alias("total_rows"),
                F.sum(F.col(cap).isNull().cast("int")).alias("missing_capacity_rows"), *link,
                F.max("effective_from").alias("latest_loaded_at"))
            .withColumn("entity", F.lit(entity))
            .withColumn("valid_link_pct", F.round(F.col("valid_link_rows") / F.col("total_rows") * 100, 1))
            .select("entity", "source_utility", "total_rows", "valid_link_rows", "valid_link_pct",
                    "missing_capacity_rows", "latest_loaded_at"))

(dq("circuit").unionByName(dq("installed_der")).unionByName(dq("planned_der"))
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{MONITOR}.data_quality"))

print("Monitoring — data_quality:")
spark.table(f"{MONITOR}.data_quality").orderBy("entity", "source_utility").show(20, False)
print("\nMonitoring build complete.")
