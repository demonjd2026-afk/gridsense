# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3 Smoke Test
# MAGIC
# MAGIC End-to-end verification that the Databricks workspace can:
# MAGIC 1. Authenticate to ADLS Gen2 via the Access Connector managed identity
# MAGIC 2. Resolve Unity Catalog schemas (`dbw_gridsense_dev.bronze`)
# MAGIC 3. Write Delta data to an external location (`abfss://bronze@...`)
# MAGIC 4. Read it back via catalog SQL
# MAGIC
# MAGIC If all 6 cells succeed, Phase 3 (Databricks workspace configuration) is complete.
# MAGIC
# MAGIC **Run order:** Run All (top toolbar) once the cluster `gridsense-dev-smoke` is attached.
# MAGIC
# MAGIC **Cleanup:** Cell 6 drops the test table. Re-running the whole notebook is safe.

# COMMAND ----------

# ============================================================================
# CELL 1: Build a small test DataFrame in memory
# ----------------------------------------------------------------------------
# Why: We don't have real data flowing yet (that's Phase 4+), but we need
# something to write. A 3-row in-memory DataFrame is enough to prove the
# write/read path.
#
# The shape mimics what real carbon-intensity events will look like once
# the producer is running.
#
# Expected output: a 3-row table with columns region, co2_forecast,
# intensity_index for GB-LON, GB-NTH, GB-SCT.
# ============================================================================

from pyspark.sql import Row

df = spark.createDataFrame(
    [
        Row(region="GB-LON", co2_forecast=180.0, intensity_index="moderate"),
        Row(region="GB-NTH", co2_forecast=95.0, intensity_index="low"),
        Row(region="GB-SCT", co2_forecast=42.0, intensity_index="very_low"),
    ]
)
df.show()

# COMMAND ----------

# ============================================================================
# CELL 2: Write the DataFrame to the bronze schema as a Delta table
# ----------------------------------------------------------------------------
# Why: This is THE moment of proof for Phase 3. If this succeeds, it means:
#   1. The Databricks cluster can authenticate to ADLS Gen2
#   2. The Access Connector managed identity is properly wired
#   3. Unity Catalog can resolve the bronze schema
#   4. The bronze external location is writable
#
# `saveAsTable` writes to the Unity Catalog catalog.schema.table path.
# Because bronze is backed by an external location, the actual data file
# lands in abfss://bronze@stgridsensedevdx0kcg.dfs.core.windows.net/
#
# `mode("overwrite")` means: if the table already exists, replace it.
# Safe for a smoke test; never use blindly on production tables.
#
# Expected output: just "Write successful". No exception.
# ============================================================================

df.write.mode("overwrite").saveAsTable("dbw_gridsense_dev.bronze.smoke_test")
print("Write successful")

# COMMAND ----------

# ============================================================================
# CELL 3: Read the table back and verify row count
# ----------------------------------------------------------------------------
# Why: Writing and reading are separate code paths in Spark/Delta. A
# successful write doesn't guarantee the data is queryable. This cell
# proves the round-trip works.
#
# Sorted by co2_forecast ASC just to make the output deterministic.
#
# Expected output:
#   - 3 rows in the order: GB-SCT (42), GB-NTH (95), GB-LON (180)
#   - "Row count: 3"
# ============================================================================

result = spark.sql("SELECT * FROM dbw_gridsense_dev.bronze.smoke_test ORDER BY co2_forecast")
result.show()
print(f"Row count: {result.count()}")

# COMMAND ----------

# ============================================================================
# CELL 4: Confirm the data physically landed in our bronze ADLS container
# ----------------------------------------------------------------------------
# Why: A common mistake is to *think* you're writing to ADLS but actually
# write to Databricks's internal managed storage (dbstoragej4zhejgjuuyzq...).
# DESCRIBE EXTENDED shows the underlying Location URI, which proves
# definitively where the data lives.
#
# What to look for in the output:
#   Find the row where col_name = "Location"
#   The data_type should be: abfss://bronze@stgridsensedevdx0kcg.dfs.core.windows.net/...
#
# If Location contains "unity-catalog-storage" or "dbstorage..." then we
# accidentally wrote to internal managed storage, not our external bronze.
# (Shouldn't happen with this catalog config, but always worth verifying.)
#
# This is the screenshot worth keeping for portfolio docs.
# ============================================================================

desc = spark.sql("DESCRIBE EXTENDED dbw_gridsense_dev.bronze.smoke_test")
desc.show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- CELL 5: List all tables in the bronze schema
# MAGIC -- ----------------------------------------------------------------------------
# MAGIC -- Why: Unity Catalog should know about our smoke_test table. This proves
# MAGIC -- catalog metadata is consistent — the table appears in normal table-listing
# MAGIC -- queries, which is what BI tools and downstream notebooks will use.
# MAGIC --
# MAGIC -- Expected output: at least one row, with tableName = "smoke_test".
# MAGIC -- ============================================================================
# MAGIC
# MAGIC SHOW TABLES IN dbw_gridsense_dev.bronze;

# COMMAND ----------

# ============================================================================
# CELL 6: Drop the smoke test table
# ----------------------------------------------------------------------------
# Why: Smoke test data shouldn't linger in bronze — it would pollute
# downstream queries and confuse future debugging. The real bronze tables
# (carbon_raw, entsoe_raw, weather_raw) come in Phase 5.
#
# `IF EXISTS` makes this safe to re-run — won't error if already dropped.
#
# DROP TABLE removes both:
#   - the catalog entry in Unity Catalog
#   - the underlying Delta files in abfss://bronze@.../smoke_test/
#
# Expected output: "Cleaned up"
# ============================================================================

spark.sql("DROP TABLE IF EXISTS dbw_gridsense_dev.bronze.smoke_test")
print("Cleaned up")
