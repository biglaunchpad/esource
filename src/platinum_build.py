# Databricks notebook source
# Platinum: refresh the two serving tables (created by setup) with INSERT OVERWRITE.
#   feeder         -> one row per feeder + DER rollups   (query: max_hosting_capacity_mw > x)
#   der_by_feeder  -> installed + planned DER per feeder (query: circuit_id = x)

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
CAT = dbutils.widgets.get("catalog")
SILVER, GOLD, PLAT = f"{CAT}.silver", f"{CAT}.gold", f"{CAT}.platinum"

# COMMAND ----------
# feeder

# COMMAND ----------
inst = spark.table(f"{GOLD}.installed_der").groupBy("source_utility", "circuit_id").agg(
    F.count("*").alias("installed_der_count"), F.round(F.sum("nameplate_rating_mw"), 3).alias("installed_capacity_mw"))
plan = spark.table(f"{GOLD}.planned_der").groupBy("source_utility", "circuit_id").agg(
    F.count("*").alias("planned_der_count"), F.round(F.sum("nameplate_rating_mw"), 3).alias("planned_capacity_mw"))

loaded = (spark.table(f"{SILVER}.circuit").filter("is_current = true")
          .select("source_utility", "circuit_id", F.col("effective_from").alias("data_loaded_at")))

feeder = (spark.table(f"{GOLD}.circuit")
          .join(inst, ["source_utility", "circuit_id"], "left")
          .join(plan, ["source_utility", "circuit_id"], "left")
          .join(loaded, ["source_utility", "circuit_id"], "left")
          .withColumn("installed_der_count", F.coalesce("installed_der_count", F.lit(0)))
          .withColumn("planned_der_count", F.coalesce("planned_der_count", F.lit(0)))
          .withColumn("total_der_count", F.col("installed_der_count") + F.col("planned_der_count"))
          .withColumn("has_capacity", F.col("max_hosting_capacity_mw").isNotNull())
          .select("circuit_id", "source_utility", "voltage_kv", "max_hosting_capacity_mw",
                  "min_hosting_capacity_mw", "has_capacity", "hca_refresh_date", "map_color", "shape_length",
                  "segment_count", "installed_der_count", "installed_capacity_mw", "planned_der_count",
                  "planned_capacity_mw", "total_der_count", "data_loaded_at"))
feeder.createOrReplaceTempView("_feeder")
spark.sql(f"INSERT OVERWRITE TABLE {PLAT}.feeder SELECT * FROM _feeder")

# COMMAND ----------
# der_by_feeder

# COMMAND ----------
inst_d = spark.table(f"{GOLD}.installed_der").select(
    "circuit_id", "source_utility", "der_id", F.lit("installed").alias("der_status"), "der_type",
    "nameplate_rating_mw", "has_valid_circuit_link", "service_address",
    F.lit(None).cast("date").alias("planned_in_service_date"), F.lit(None).cast("string").alias("project_status"))
plan_d = spark.table(f"{GOLD}.planned_der").select(
    "circuit_id", "source_utility", "der_id", F.lit("planned").alias("der_status"), "der_type",
    "nameplate_rating_mw", "has_valid_circuit_link", F.lit(None).cast("string").alias("service_address"),
    "planned_in_service_date", "project_status")
inst_d.unionByName(plan_d).createOrReplaceTempView("_der")
spark.sql(f"INSERT OVERWRITE TABLE {PLAT}.der_by_feeder SELECT * FROM _der")

# COMMAND ----------
# self-check: the two required queries

# COMMAND ----------
print("feeders:", spark.table(f"{PLAT}.feeder").count())

print("\nRequired query 1 — feeders with max hosting capacity > 8 MW:")
spark.sql(f"""SELECT circuit_id, max_hosting_capacity_mw, installed_der_count, planned_der_count
              FROM {PLAT}.feeder WHERE max_hosting_capacity_mw > 8
              ORDER BY max_hosting_capacity_mw DESC LIMIT 5""").show(5, False)

fdr = (spark.table(f"{PLAT}.der_by_feeder").filter("has_valid_circuit_link")
       .groupBy("circuit_id").count().orderBy(F.desc("count")).first()[0])
print(f"Required query 2 — all installed + planned DER for feeder {fdr}:")
spark.sql(f"""SELECT der_id, der_status, der_type, nameplate_rating_mw
              FROM {PLAT}.der_by_feeder WHERE circuit_id = '{fdr}' LIMIT 10""").show(10, False)

print("\nFeeder freshness sample (per-feeder load timestamp + capacity flag):")
spark.sql(f"""SELECT circuit_id, source_utility, max_hosting_capacity_mw, has_capacity, hca_refresh_date, data_loaded_at
              FROM {PLAT}.feeder ORDER BY data_loaded_at DESC NULLS LAST LIMIT 5""").show(5, False)
print("\nPlatinum build complete.")
