# Databricks notebook source
# Bronze: Auto Loader reads each source's monthly CSV drop and appends to a Bronze
# table. Everything stays STRING here; typing and conforming happen in Silver.
# availableNow + checkpoints keep re-runs incremental (only new files get processed).

# COMMAND ----------
dbutils.widgets.text("catalog", "iedr_dev")
CATALOG = dbutils.widgets.get("catalog")

LANDING = f"/Volumes/{CATALOG}/bronze/iedr_landing"
BRONZE  = f"{CATALOG}.bronze"

# Each source's landing subfolder name == its target Bronze table name.
SOURCES = [
    "u1_circuits",
    "u2_circuits",
    "u1_installed_der",
    "u2_installed_der",
    "u1_planned_der",
    "u2_planned_der",
]

# COMMAND ----------
from pyspark.sql import functions as F


def ingest(source: str) -> None:
    """Incrementally load one source's new files into its Bronze table."""
    src_path   = f"{LANDING}/{source}"
    schema_loc = f"{LANDING}/_schemas/{source}"
    checkpoint = f"{LANDING}/_checkpoints/{source}"
    target     = f"{BRONZE}.{source}"

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")   # keep every column as STRING
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("rescuedDataColumn", "_rescued_data")      # off-schema columns land here, not dropped
        .load(src_path)
        # --- audit / lineage columns ---
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.date_format(F.current_timestamp(), "yyyyMMddHHmmss"))
    )

    query = (
        stream.writeStream
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()
    print(f"  {source:18s} -> {target}  (total rows now: {spark.table(target).count():,})")


# COMMAND ----------
print(f"Bronze ingestion into catalog: {CATALOG}\n")
for s in SOURCES:
    ingest(s)
print("\nBronze ingestion complete.")
