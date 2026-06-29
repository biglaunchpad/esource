# Databricks notebook source
# Setup: create the Silver SCD2 tables + Platinum serving tables, and seed the DER
# type crosswalk. Run on first deploy and whenever the schema changes (add columns
# here as ALTER). Not part of the daily pipeline. Idempotent (CREATE IF NOT EXISTS).

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
CAT = dbutils.widgets.get("catalog")

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.silver.circuit (
  circuit_bk                 STRING  COMMENT 'business key: source_utility || "|" || circuit_id',
  source_utility             STRING  COMMENT 'U1 | U2',
  circuit_id                 STRING  COMMENT 'U1 CIRCUIT (rolled up from segments) | U2 Master_CDF',
  voltage_kv                 DOUBLE,
  max_hosting_capacity_mw    DOUBLE  COMMENT 'circuit-level MAX hosting capacity (U1 = MAX over segments)',
  min_hosting_capacity_mw    DOUBLE,
  hca_refresh_date           DATE,
  map_color                  STRING,
  dg_connected_since_refresh DOUBLE  COMMENT 'U2 only; NULL for U1',
  shape_length               DOUBLE  COMMENT 'U1 = SUM over segments',
  segment_count              INT     COMMENT 'U1 = count of rolled-up segments; U2 = 1',
  notes                      STRING,
  source_file                STRING,
  load_batch_id              STRING,
  _ingested_at               TIMESTAMP,
  effective_from             TIMESTAMP,
  effective_to               TIMESTAMP,
  is_current                 BOOLEAN,
  row_hash                   STRING  COMMENT 'sha2 over business columns; drives SCD2 change detection'
)
CLUSTER BY (circuit_bk)
COMMENT 'Conformed circuit-grain common model with SCD Type 2 history'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.enableDeletionVectors'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.silver.installed_der (
  der_bk                  STRING  COMMENT 'business key: source_utility || "|" || der_id',
  source_utility          STRING,
  der_id                  STRING  COMMENT 'U1 ProjectID | U2 DER_ID',
  circuit_id              STRING  COMMENT 'FK to circuit; NULLABLE when linkage missing',
  has_valid_circuit_link  BOOLEAN COMMENT 'TRUE when circuit_id resolves to silver.circuit (DQ signal)',
  der_type                STRING  COMMENT 'canonical (via der_type_map)',
  der_type_source         STRING,
  nameplate_rating_mw     DOUBLE,
  technology_mix          MAP<STRING, DOUBLE> COMMENT 'U1 one-hot MW breakdown; NULL for U2',
  service_address         STRING,
  interconnection_cost    DOUBLE,
  source_file             STRING,
  load_batch_id           STRING,
  _ingested_at            TIMESTAMP,
  effective_from          TIMESTAMP,
  effective_to            TIMESTAMP,
  is_current              BOOLEAN,
  row_hash                STRING
)
CLUSTER BY (circuit_id)
COMMENT 'Conformed installed DER with SCD Type 2 history'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.enableDeletionVectors'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.silver.planned_der (
  der_bk                   STRING  COMMENT 'business key: source_utility || "|" || der_id',
  source_utility           STRING,
  der_id                   STRING  COMMENT 'U1 ProjectID | U2 INTERCONNECTION_QUEUE_REQUEST_ID',
  circuit_id               STRING  COMMENT 'FK to circuit; NULLABLE',
  has_valid_circuit_link   BOOLEAN,
  der_type                 STRING  COMMENT 'canonical',
  der_type_source          STRING,
  nameplate_rating_mw      DOUBLE,
  inverter_nameplate_mw    DOUBLE  COMMENT 'U2 only',
  planned_in_service_date  DATE,
  completion_date          DATE    COMMENT 'U1 only',
  project_status           STRING  COMMENT 'U1 ProjectStatus | U2 DER_STATUS',
  status_rationale         STRING  COMMENT 'U2 only',
  queue_position           STRING  COMMENT 'U2 only',
  total_mw_for_substation  DOUBLE  COMMENT 'U2 only',
  source_file              STRING,
  load_batch_id            STRING,
  _ingested_at             TIMESTAMP,
  effective_from           TIMESTAMP,
  effective_to             TIMESTAMP,
  is_current               BOOLEAN,
  row_hash                 STRING
)
CLUSTER BY (circuit_id)
COMMENT 'Conformed planned DER (interconnection queue) with SCD Type 2 history'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true','delta.enableDeletionVectors'='true')
""")

# COMMAND ----------
# Platinum serving tables, clustered on the column each query filters on.

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.platinum.feeder (
  circuit_id STRING, source_utility STRING, voltage_kv DOUBLE,
  max_hosting_capacity_mw DOUBLE, min_hosting_capacity_mw DOUBLE, has_capacity BOOLEAN, hca_refresh_date DATE,
  map_color STRING, shape_length DOUBLE, segment_count INT,
  installed_der_count BIGINT, installed_capacity_mw DOUBLE,
  planned_der_count BIGINT, planned_capacity_mw DOUBLE, total_der_count BIGINT,
  data_loaded_at TIMESTAMP,
  pipeline_version STRING
)
CLUSTER BY (max_hosting_capacity_mw)
COMMENT 'Feeder current state + DER rollups + per-feeder freshness. Serves: max_hosting_capacity_mw > :x'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CAT}.platinum.der_by_feeder (
  circuit_id STRING, source_utility STRING, der_id STRING, der_status STRING, der_type STRING,
  nameplate_rating_mw DOUBLE, has_valid_circuit_link BOOLEAN, service_address STRING,
  planned_in_service_date DATE, project_status STRING
)
CLUSTER BY (circuit_id)
COMMENT 'Installed + planned DER per feeder. Serves: circuit_id = :x'
TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
""")

# COMMAND ----------
# DER type crosswalk. This list is the source of truth; the table is overwritten from it.
SEED = [
    ("U1","RESPHOTO","SOLAR"), ("U1","NRESPHOTO","SOLAR"), ("U1","SolarPV","SOLAR"),
    ("U1","PVESS","HYBRID"),   ("U1","Hybrid","HYBRID"),
    ("U1","RESWIND","WIND"),   ("U1","COMWIND","WIND"),   ("U1","FARMWIND","WIND"), ("U1","Wind","WIND"),
    ("U1","EnergyStorageSystem","STORAGE"),
    ("U1","CHP","CHP"),        ("U1","CombinedHeatandPower","CHP"),
    ("U1","FUELCELL","FUEL_CELL"), ("U1","FuelCell","FUEL_CELL"),
    ("U1","langas","BIOGAS"),  ("U1","FarmWaste","FARM_WASTE"),
    ("U1","Hydro","HYDRO"),    ("U1","GasTurbine","NATURAL_GAS"), ("U1","MicroTurbine","MICRO_TURBINE"),
    ("U1","SynchronousGenerator","SYNCHRONOUS_GEN"), ("U1","InductionGenerator","INDUCTION_GEN"),
    ("U1","InternalCombustionEngine","ICE"), ("U1","SteamTurbine","STEAM"), ("U1","Other","OTHER"),
    ("U2","Solar","SOLAR"),    ("U2","Wind","WIND"),     ("U2","Hydro","HYDRO"),
    ("U2","Battery Add-On","STORAGE"), ("U2","Natural Gas","NATURAL_GAS"),
    ("U2","Steam","STEAM"),    ("U2","Bio Gas","BIOGAS"),
]
(spark.createDataFrame(SEED, "source_utility string, der_type_source string, der_type_canonical string")
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CAT}.silver.der_type_map"))

print(f"Setup complete: Silver + Platinum tables created and DER taxonomy seeded ({len(SEED)} mappings) in {CAT}")
