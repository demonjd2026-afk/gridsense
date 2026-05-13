# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - Open-Meteo
# MAGIC
# MAGIC Streams the Open-Meteo weather producer's output from Event Hubs into
# MAGIC `dbw_gridsense_dev.bronze.open_meteo`.
# MAGIC
# MAGIC **Source:** topic `open-meteo` in namespace `evhns-gridsense-dev`
# MAGIC **Sink:** Delta table partitioned by `event_date`
# MAGIC **Trigger:** `availableNow=True` - the Asset Bundle job schedules
# MAGIC this hourly. Weather data updates hourly upstream so a 15-min
# MAGIC cadence on Bronze ingestion (matching the producer poll interval)
# MAGIC would only batch the same offsets.

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

TOPIC = "open-meteo"
TABLE = f"{catalog}.bronze.open_meteo"
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
      COUNT(DISTINCT kafka_key) AS distinct_cities
    FROM {TABLE}
""")
result.show(truncate=False)
