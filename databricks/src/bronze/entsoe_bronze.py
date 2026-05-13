# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - ENTSO-E
# MAGIC
# MAGIC Streams the ENTSO-E producer's output from Event Hubs into
# MAGIC `dbw_gridsense_dev.bronze.entsoe`.
# MAGIC
# MAGIC **Source:** topic `entsoe` in namespace `evhns-gridsense-dev`
# MAGIC **Sink:** Delta table partitioned by `event_date`
# MAGIC **Trigger:** `availableNow=True` - the Asset Bundle job schedules
# MAGIC this hourly, matching the producer's hourly poll cadence.
# MAGIC
# MAGIC ENTSO-E publishes generation data with a 2-3 hour TSO publication
# MAGIC lag and occasionally back-publishes corrections. Bronze is append-only
# MAGIC and preserves the raw envelope including ingested_at and event_time;
# MAGIC late-arrival reconciliation happens in Silver via Delta MERGE on
# MAGIC (country, period_start).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
dbutils.widgets.text("eh_namespace", "evhns-gridsense-dev")
dbutils.widgets.text("kafka_bootstrap", "evhns-gridsense-dev.servicebus.windows.net:9093")
dbutils.widgets.text("bronze_root", "abfss://bronze@stgridsensedevdx0kcg.dfs.core.windows.net/")

catalog = dbutils.widgets.get("catalog")
namespace = dbutils.widgets.get("eh_namespace")
bootstrap = dbutils.widgets.get("kafka_bootstrap")
bronze_root = dbutils.widgets.get("bronze_root").rstrip("/")

TOPIC = "entsoe"
TABLE = f"{catalog}.bronze.entsoe"
CHECKPOINT = f"{bronze_root}/_checkpoints/{TOPIC}/"

print(f"Topic:      {TOPIC}")
print(f"Table:      {TABLE}")
print(f"Checkpoint: {CHECKPOINT}")

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream

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

# COMMAND ----------

result = spark.sql(f"""
    SELECT
      COUNT(*) AS row_count,
      MAX(kafka_timestamp) AS latest_event,
      MIN(kafka_timestamp) AS earliest_event,
      COUNT(DISTINCT event_date) AS partition_count,
      COUNT(DISTINCT kafka_key) AS distinct_countries
    FROM {TABLE}
""")
result.show(truncate=False)
