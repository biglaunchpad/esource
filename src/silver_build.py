# Databricks notebook source
# Silver: read the six Bronze tables, conform both utilities into one model
# (DER taxonomy, U1 segment->circuit rollup, date/number cleanup, circuit-link flag),
# then apply SCD2 so monthly new+changed records keep history.

# COMMAND ----------
from pyspark.sql import Window, functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "workspace")
CAT = dbutils.widgets.get("catalog")
BRONZE, SILVER = f"{CAT}.bronze", f"{CAT}.silver"
LOAD_TS = spark.sql("SELECT current_timestamp()").first()[0]   # one consistent load timestamp per run

# COMMAND ----------
# helpers

# COMMAND ----------
def clean_cols(df):
    """Strip BOM/whitespace from column names (U2 DER files carry a BOM)."""
    return df.toDF(*[c.strip().replace("\ufeff", "") for c in df.columns])

def src(name):
    return clean_cols(spark.table(f"{BRONZE}.{name}"))

def parse_date(colname):
    s = F.split(F.col(colname), " ").getItem(0)            # drop any trailing time component
    return (F.when(s.rlike(r"^\d{4}-\d{2}-\d{2}$"),     F.to_date(s, "yyyy-MM-dd"))
             .when(s.rlike(r"^\d{4}/\d{2}/\d{2}$"),     F.to_date(s, "yyyy/MM/dd"))
             .when(s.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"), F.to_date(s, "M/d/yyyy"))
             .otherwise(F.lit(None).cast("date")))

def row_hash(cols):
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]), 256)

def dbl(colname):
    """Safe string->double: only cast values that look numeric, else NULL (no ANSI throw)."""
    c = F.col(colname)
    return F.when(c.rlike(r"^\s*-?\d+(\.\d+)?([eE][-+]?\d+)?\s*$"), c.cast("double")).otherwise(F.lit(None).cast("double"))

TECH = {"SolarPV": "SOLAR", "EnergyStorageSystem": "STORAGE", "Wind": "WIND",
        "MicroTurbine": "MICRO_TURBINE", "SynchronousGenerator": "SYNCHRONOUS_GEN",
        "InductionGenerator": "INDUCTION_GEN", "FarmWaste": "FARM_WASTE", "FuelCell": "FUEL_CELL",
        "CombinedHeatandPower": "CHP", "GasTurbine": "NATURAL_GAS", "Hydro": "HYDRO",
        "InternalCombustionEngine": "ICE", "SteamTurbine": "STEAM", "Other": "OTHER"}
# Note: source "Hybrid" is a Y/N indicator, not MW, so it is excluded from technology_mix;
# hybrid projects are still classified via der_type.

def tech_mix():
    pairs = []
    for s, c in TECH.items():
        pairs += [F.lit(c), dbl(s)]
    return F.map_filter(F.create_map(*pairs), lambda k, v: v.isNotNull() & (v > F.lit(0)))

def scd2_apply(target, incoming, keys):
    """Standard SCD Type 2: close changed current rows, then append new versions."""
    cur = spark.table(target).filter("is_current = true").select(*keys, F.col("row_hash").alias("_cur_hash"))
    j = incoming.join(cur, keys, "left")
    to_insert = j.filter("_cur_hash IS NULL OR _cur_hash <> row_hash").drop("_cur_hash")
    changed = j.filter("_cur_hash IS NOT NULL AND _cur_hash <> row_hash").select(*keys)
    n = to_insert.count()                                  # count before the target mutates (set is stable across the merge)

    if changed.take(1):  # close the prior current version of changed keys
        cond = " AND ".join([f"t.{k} = c.{k}" for k in keys]) + " AND t.is_current = true"
        (DeltaTable.forName(spark, target).alias("t").merge(changed.alias("c"), cond)
         .whenMatchedUpdate(set={"is_current": F.lit(False),
                                 "effective_to": F.lit(LOAD_TS).cast("timestamp")}).execute())

    final = (to_insert.withColumn("effective_from", F.lit(LOAD_TS).cast("timestamp"))
             .withColumn("effective_to", F.lit(None).cast("timestamp"))
             .withColumn("is_current", F.lit(True)))
    cols = spark.table(target).columns
    final.select(*cols).write.format("delta").mode("append").saveAsTable(target)
    return n

def valid_circuits():
    return (spark.table(f"{SILVER}.circuit").filter("is_current = true")
            .select("source_utility", "circuit_id").distinct().withColumn("_v", F.lit(True)))

# COMMAND ----------
# conform circuit: U1 segments roll up to circuit grain, U2 is already circuit-grain

# COMMAND ----------
HC_CIRCUIT = ["voltage_kv", "max_hosting_capacity_mw", "min_hosting_capacity_mw", "hca_refresh_date",
              "map_color", "dg_connected_since_refresh", "shape_length", "segment_count", "notes"]

def conform_circuit():
    w = Window.partitionBy("Circuits_Phase3_CIRCUIT", "NYHCPV_csv_NSECTION").orderBy(F.col("_ingested_at").desc())
    u1 = (src("u1_circuits").withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
          .groupBy("Circuits_Phase3_CIRCUIT").agg(
              F.max(dbl("NYHCPV_csv_FVOLTAGE")).alias("voltage_kv"),
              F.max(dbl("NYHCPV_csv_FMAXHC")).alias("max_hosting_capacity_mw"),
              F.min(dbl("NYHCPV_csv_FMINHC")).alias("min_hosting_capacity_mw"),
              F.max(parse_date("NYHCPV_csv_FHCADATE")).alias("hca_refresh_date"),
              F.max("NYHCPV_csv_NMAPCOLOR").alias("map_color"),
              F.lit(None).cast("double").alias("dg_connected_since_refresh"),
              F.round(F.sum(dbl("Shape_Length")), 3).alias("shape_length"),
              F.count(F.lit(1)).cast("int").alias("segment_count"),
              F.max("NYHCPV_csv_FNOTES").alias("notes"),
              F.max("_source_file").alias("source_file"),
              F.max("_batch_id").alias("load_batch_id"),
              F.max("_ingested_at").alias("_ingested_at"))
          .withColumn("source_utility", F.lit("U1"))
          .withColumnRenamed("Circuits_Phase3_CIRCUIT", "circuit_id"))

    w2 = Window.partitionBy("Master_CDF").orderBy(F.col("_ingested_at").desc())
    u2 = (src("u2_circuits").withColumn("_rn", F.row_number().over(w2)).filter("_rn = 1").select(
            F.col("Master_CDF").alias("circuit_id"), F.lit("U2").alias("source_utility"),
            dbl("feeder_voltage").alias("voltage_kv"),
            dbl("feeder_max_hc").alias("max_hosting_capacity_mw"),
            dbl("feeder_min_hc").alias("min_hosting_capacity_mw"),
            parse_date("hca_refresh_date").alias("hca_refresh_date"),
            F.col("color").alias("map_color"),
            dbl("feeder_dg_connected_since_refresh").alias("dg_connected_since_refresh"),
            dbl("shape_length").alias("shape_length"),
            F.lit(1).alias("segment_count"), F.lit(None).cast("string").alias("notes"),
            F.col("_source_file").alias("source_file"), F.col("_batch_id").alias("load_batch_id"),
            F.col("_ingested_at").alias("_ingested_at")))

    return (u1.unionByName(u2)
            .withColumn("circuit_bk", F.concat_ws("|", "source_utility", "circuit_id"))
            .withColumn("row_hash", row_hash(HC_CIRCUIT)))

# COMMAND ----------
# conform installed DER

# COMMAND ----------
HC_INST = ["circuit_id", "der_type", "nameplate_rating_mw", "service_address", "interconnection_cost"]

def conform_installed(dtm):
    w = Window.partitionBy("ProjectID").orderBy(F.col("_ingested_at").desc())
    u1 = (src("u1_installed_der").withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").select(
            F.col("ProjectID").alias("der_id"), F.lit("U1").alias("source_utility"),
            F.col("ProjectCircuitID").alias("circuit_id"), F.col("ProjectType").alias("der_type_source"),
            dbl("NamePlateRating").alias("nameplate_rating_mw"),
            tech_mix().alias("technology_mix"), F.lit(None).cast("string").alias("service_address"),
            (F.coalesce(dbl("TotalChargesCESIR"), F.lit(0.0)) +
             F.coalesce(dbl("TotalChargesConstruction"), F.lit(0.0))).alias("interconnection_cost"),
            F.col("_source_file").alias("source_file"), F.col("_batch_id").alias("load_batch_id"),
            F.col("_ingested_at").alias("_ingested_at")))

    w2 = Window.partitionBy("DER_ID").orderBy(F.col("_ingested_at").desc())
    u2 = (src("u2_installed_der").withColumn("_rn", F.row_number().over(w2)).filter("_rn = 1").select(
            F.col("DER_ID").alias("der_id"), F.lit("U2").alias("source_utility"),
            F.col("DER_INTERCONNECTION_LOCATION").alias("circuit_id"), F.col("DER_TYPE").alias("der_type_source"),
            dbl("DER_NAMEPLATE_RATING").alias("nameplate_rating_mw"),
            F.lit(None).cast("map<string,double>").alias("technology_mix"),
            F.col("SERVICE_STREET_ADDRESS").alias("service_address"),
            dbl("INTERCONNECTION_COST").alias("interconnection_cost"),
            F.col("_source_file").alias("source_file"), F.col("_batch_id").alias("load_batch_id"),
            F.col("_ingested_at").alias("_ingested_at")))

    return (u1.unionByName(u2)
            .join(dtm, ["source_utility", "der_type_source"], "left")
            .withColumn("der_type", F.coalesce(F.col("der_type_canonical"), F.lit("OTHER"))).drop("der_type_canonical")
            .join(valid_circuits(), ["source_utility", "circuit_id"], "left")
            .withColumn("has_valid_circuit_link", F.coalesce(F.col("_v"), F.lit(False))).drop("_v")
            .withColumn("der_bk", F.concat_ws("|", "source_utility", "der_id"))
            .withColumn("row_hash", row_hash(HC_INST)))

# COMMAND ----------
# conform planned DER

# COMMAND ----------
HC_PLAN = ["circuit_id", "der_type", "nameplate_rating_mw", "inverter_nameplate_mw", "planned_in_service_date",
           "completion_date", "project_status", "status_rationale", "queue_position", "total_mw_for_substation"]

def conform_planned(dtm):
    w = Window.partitionBy("ProjectID").orderBy(F.col("_ingested_at").desc())
    u1 = (src("u1_planned_der").withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").select(
            F.col("ProjectID").alias("der_id"), F.lit("U1").alias("source_utility"),
            F.col("ProjectCircuitID").alias("circuit_id"), F.col("ProjectType").alias("der_type_source"),
            dbl("NamePlateRating").alias("nameplate_rating_mw"),
            F.lit(None).cast("double").alias("inverter_nameplate_mw"),
            parse_date("InServiceDate").alias("planned_in_service_date"),
            parse_date("CompletionDate").alias("completion_date"),
            F.col("ProjectStatus").alias("project_status"), F.lit(None).cast("string").alias("status_rationale"),
            F.lit(None).cast("string").alias("queue_position"), F.lit(None).cast("double").alias("total_mw_for_substation"),
            F.col("_source_file").alias("source_file"), F.col("_batch_id").alias("load_batch_id"),
            F.col("_ingested_at").alias("_ingested_at")))

    w2 = Window.partitionBy("INTERCONNECTION_QUEUE_REQUEST_ID").orderBy(F.col("_ingested_at").desc())
    u2 = (src("u2_planned_der").withColumn("_rn", F.row_number().over(w2)).filter("_rn = 1").select(
            F.col("INTERCONNECTION_QUEUE_REQUEST_ID").alias("der_id"), F.lit("U2").alias("source_utility"),
            F.col("DER_INTERCONNECTION_LOCATION").alias("circuit_id"), F.col("DER_TYPE").alias("der_type_source"),
            dbl("DER_NAMEPLATE_RATING").alias("nameplate_rating_mw"),
            dbl("INVERTER_NAMEPLATE_RATING").alias("inverter_nameplate_mw"),
            parse_date("PLANNED_INSTALLATION_DATE").alias("planned_in_service_date"),
            F.lit(None).cast("date").alias("completion_date"),
            F.col("DER_STATUS").alias("project_status"), F.col("DER_STATUS_RATIONALE").alias("status_rationale"),
            F.col("INTERCONNECTION_QUEUE_POSITION").alias("queue_position"),
            dbl("TOTAL_MW_FOR_SUBSTATION").alias("total_mw_for_substation"),
            F.col("_source_file").alias("source_file"), F.col("_batch_id").alias("load_batch_id"),
            F.col("_ingested_at").alias("_ingested_at")))

    return (u1.unionByName(u2)
            .join(dtm, ["source_utility", "der_type_source"], "left")
            .withColumn("der_type", F.coalesce(F.col("der_type_canonical"), F.lit("OTHER"))).drop("der_type_canonical")
            .join(valid_circuits(), ["source_utility", "circuit_id"], "left")
            .withColumn("has_valid_circuit_link", F.coalesce(F.col("_v"), F.lit(False))).drop("_v")
            .withColumn("der_bk", F.concat_ws("|", "source_utility", "der_id"))
            .withColumn("row_hash", row_hash(HC_PLAN)))

# COMMAND ----------
# run order: circuit first, since the DER link flag checks against silver.circuit

# COMMAND ----------
print(f"Silver build into {SILVER} @ {LOAD_TS}\n")
dtm = spark.table(f"{SILVER}.der_type_map")

print("circuit       : merged", scd2_apply(f"{SILVER}.circuit", conform_circuit(), ["circuit_bk"]))
print("installed_der : merged", scd2_apply(f"{SILVER}.installed_der", conform_installed(dtm), ["der_bk"]))
print("planned_der   : merged", scd2_apply(f"{SILVER}.planned_der", conform_planned(dtm), ["der_bk"]))

print("\n-- current-row counts --")
for t in ["circuit", "installed_der", "planned_der"]:
    print(f"  silver.{t:14s} current = {spark.table(f'{SILVER}.{t}').filter('is_current').count():,}")
print("\nSilver build complete.")
