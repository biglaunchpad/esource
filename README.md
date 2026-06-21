# IEDR Utility Data Lakehouse

A Databricks medallion lakehouse that ingests circuit, installed-DER, and planned-DER
data from two New York utilities, conforms it into one model, and serves two queries:

- feeders with hosting capacity over a threshold
- installed + planned DER for a given feeder

It's defined as a Databricks Asset Bundle: the schemas, landing volume, and two jobs
deploy with `databricks bundle deploy`. The catalog is assumed to exist (the platform
team owns catalogs); the bundle deploys into it.

## Layout

```
iedr-utility-lakehouse/
├── databricks.yml              # bundle: schemas, volume, 2 jobs, dev/test/prod targets
├── .github/workflows/ci.yml    # deploy on PR (dev), merge (test), tag (prod)
├── src/
│   ├── setup.py                # create Silver + Platinum tables, seed the DER type map
│   ├── bronze_ingest.py        # Auto Loader -> Bronze
│   ├── silver_build.py         # conform + SCD2
│   ├── gold_build.py           # current-state projection
│   ├── platinum_build.py       # load the two serving tables
│   └── monitoring_build.py     # data-quality snapshot
└── README.md
```

## Two jobs

- **iedr_setup** — all the DDL. Creates the Silver SCD2 tables and the Platinum serving
  tables and seeds the DER type crosswalk. Run once to bootstrap, and again when the schema
  changes (add columns as `ALTER TABLE`). Not scheduled.
- **iedr_pipeline** — the daily/monthly ETL, data only. Each stage is its own task:
  `bronze_ingest -> silver_build -> gold_build -> platinum_build`, with `monitoring_build`
  off `silver_build` in parallel.

Bronze tables are created by Auto Loader. Gold and `monitoring.data_quality` are rebuilt
each run by their tasks. Only the Silver and Platinum tables — the ones that carry history
or clustering — are pre-created by setup.

## Environments

Same code, three targets, differing by catalog. test and prod run as a service principal.

| Target | Catalog     | Notes                      |
|--------|-------------|----------------------------|
| dev    | `workspace` | default                    |
| test   | `iedr_test` | run_as service principal   |
| prod   | `iedr_prod` | production mode, run_as SP  |

## Run it (dev)

From the repo root, using your CLI profile (`iedr-trial` here).

```bash
# 1. deploy the bundle (schemas, volume, jobs)
databricks bundle deploy -t dev -p iedr-trial

# 2. create the tables + seed the type map (once, and after any schema change)
databricks bundle run iedr_setup -t dev -p iedr-trial

# 3. drop the month's files into the landing volume
V=dbfs:/Volumes/workspace/bronze/iedr_landing
databricks fs cp ./data/utility1_circuits.csv     $V/u1_circuits/      -p iedr-trial
databricks fs cp ./data/utility2_circuits.csv     $V/u2_circuits/      -p iedr-trial
databricks fs cp ./data/utility1_install_der.csv  $V/u1_installed_der/ -p iedr-trial
databricks fs cp ./data/utility2_install_der.csv  $V/u2_installed_der/ -p iedr-trial
databricks fs cp ./data/utility1_planned_der.csv  $V/u1_planned_der/   -p iedr-trial
databricks fs cp ./data/utility2_planned_der.csv  $V/u2_planned_der/   -p iedr-trial

# 4. run the pipeline
databricks bundle run iedr_pipeline -t dev -p iedr-trial
```

## What to expect

Bronze counts from the sample data: 64,539 / 1,909 / 13,727 / 25,537 / 1,688 / 30,957.

Silver current-state counts:

```sql
SELECT 'circuit' t, count(*) FROM workspace.silver.circuit WHERE is_current
UNION ALL SELECT 'installed_der', count(*) FROM workspace.silver.installed_der WHERE is_current
UNION ALL SELECT 'planned_der',   count(*) FROM workspace.silver.planned_der   WHERE is_current;
```

circuit **2,200** (U1's 64,539 segments rolled up to 291, plus U2's 1,909), installed_der
**39,263** (one duplicate dropped), planned_der **32,645**.

The two serving queries:

```sql
SELECT * FROM workspace.platinum.feeder WHERE max_hosting_capacity_mw > 8;
SELECT * FROM workspace.platinum.der_by_feeder WHERE circuit_id = '36_30_45151';
```

Data-quality metrics (separate `monitoring` schema, not part of the served product):

```sql
SELECT * FROM workspace.monitoring.data_quality ORDER BY entity, source_utility;
```

Both serving tables are liquid-clustered on the column they're filtered by and have Change
Data Feed on for a future Lakebase sync.

## CI/CD

`.github/workflows/ci.yml` deploys the bundle on:

- pull request -> validate + deploy **dev**
- merge to `main` -> deploy **test** + run setup
- tag `v*` -> deploy **prod** + run setup (gated by the `prod` environment)

Auth is GitHub OIDC (no tokens in the repo). Set once in GitHub:

- variable `DATABRICKS_HOST` — workspace URL
- secret `DATABRICKS_CLIENT_ID` — service principal UUID
- variable `SERVICE_PRINCIPAL_ID` — same SP app ID (used for `run_as`)
- a GitHub Actions federation policy on the SP, subject `repo:biglaunchpad/esource:environment:<env>`
- `dev` / `test` / `prod` environments, with a required reviewer on `prod`

test/prod deploy into `iedr_test` / `iedr_prod`. On a single trial workspace only the
`workspace` catalog exists, so dev is the live target; test/prod come online once those
catalogs (or separate workspaces) exist.
