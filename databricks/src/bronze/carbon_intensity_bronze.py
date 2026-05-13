# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - Carbon Intensity
# MAGIC
# MAGIC Streams the UK Carbon Intensity producer's output from Event Hubs into
# MAGIC `dbw_gridsense_dev.bronze.carbon_intensity`.
# MAGIC
# MAGIC **Source:** topic `carbon-intensity` in namespace `evhns-gridsense-dev`
# MAGIC **Sink:** Delta table partitioned by `event_date`
# MAGIC **Trigger:** `availableNow=True` — processes all new events then exits.
# MAGIC The Asset Bundle job schedules this hourly.
# MAGIC
# MAGIC **Auth:** cluster managed identity (Databricks Access Connector UAMI)
# MAGIC has the `Azure Event Hubs Data Receiver` role on the namespace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Wired from Asset Bundle variables. Override in the notebook UI for
# MAGIC ad-hoc runs.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
dbutils.widgets.text("eh_namespace", "evhns-gridsense-dev")
dbutils.widgets.text("kafka_bootstrap", "evhns-gridsense-dev.servicebus.windows.net:9093")
dbutils.widgets.text("bronze_root", "abfss://bronze@stgridsensedevdx0kcg.dfs.core.windows.net/")

catalog = dbutils.widgets.get("catalog")
namespace = dbutils.widgets.get("eh_namespace")
bootstrap = dbutils.widgets.get("kafka_bootstrap")
bronze_root = dbutils.widgets.get("bronze_root").rstrip("/")

TOPIC = "carbon-intensity"
TABLE = f"{catalog}.bronze.carbon_intensity"
CHECKPOINT = f"{bronze_root}/_checkpoints/{TOPIC}/"

print(f"Topic:      {TOPIC}")
print(f"Table:      {TABLE}")
print(f"Checkpoint: {CHECKPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers
# MAGIC The shared `common.py` lives alongside this notebook. We import it
# MAGIC via `%run` because Asset Bundle-deployed notebooks live in the
# MAGIC workspace as siblings, not as a Python package.

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream
# MAGIC Reads from Event Hubs, enriches with Bronze columns, writes to Delta.
# MAGIC With `availableNow=True` this is a one-shot batch (processes all new
# MAGIC offsets, then `awaitTermination` returns). The job scheduler re-runs
# MAGIC it hourly.

# COMMAND ----------

raw = read_bronze_stream(spark, dbutils, bootstrap, namespace, TOPIC)
enriched = add_bronze_columns(raw)

write_bronze(
    df=enriched,
    table_fqn=TABLE,
    checkpoint_path=CHECKPOINT,
    trigger_available_now=True,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC Quick row count and most-recent timestamp so the job log shows
# MAGIC something useful besides "completed successfully".

# COMMAND ----------

result = spark.sql(f"""
    SELECT
      COUNT(*) AS row_count,
      MAX(kafka_timestamp) AS latest_event,
      MIN(kafka_timestamp) AS earliest_event,
      COUNT(DISTINCT event_date) AS partition_count
    FROM {TABLE}
""")
result.show(truncate=False)
