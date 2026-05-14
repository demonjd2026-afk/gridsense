# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - dim_time
# MAGIC
# MAGIC Hourly time dimension covering 2026-01-01 UTC through 2028-01-01 UTC
# MAGIC (~17,544 rows). Sized to actual data window (producers came online
# MAGIC May 2026) plus a 20-month forward horizon for ML forecasting.
# MAGIC
# MAGIC Why generate it instead of inferring from facts: facts answer "what
# MAGIC happened" but a time dim lets you query gaps ("which hours had NO
# MAGIC generation data?") and supports forecast-window joins ("project
# MAGIC carbon intensity for the next 168 hours") without needing the
# MAGIC underlying facts to already exist.
# MAGIC
# MAGIC All datetime attributes are derived in UTC. Local-time variants for
# MAGIC each country live in dim_country.timezone_offset_*; downstream
# MAGIC queries that need "Berlin business hours" compose the two dims.
# MAGIC
# MAGIC Overwrite-each-run; takes ~10s.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

TARGET_TABLE = f"{catalog}.gold.dim_time"
START_TS = "2026-01-01 00:00:00"
END_TS = "2028-01-01 00:00:00"

print(f"Target: {TARGET_TABLE}")
print(f"Range:  {START_TS} -> {END_TS}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Spark sequence() with interval generates an array of timestamps; explode
# fans it out to one row per hour. The whole chain is ~17,500 rows so it
# fits comfortably on serverless without partition tuning.

base = spark.range(1).select(
    F.explode(
        F.sequence(
            F.to_timestamp(F.lit(START_TS)),
            F.to_timestamp(F.lit(END_TS)),
            F.expr("INTERVAL 1 HOUR"),
        )
    ).alias("hour_utc")
)

# COMMAND ----------

# Analytical attributes. Naming: prefix with the unit/category so the
# columns sort sensibly in any BI tool that alphabetises them.

enriched = (
    base
    # Date / calendar attributes
    .withColumn("date_utc", F.to_date("hour_utc"))
    .withColumn("year", F.year("hour_utc"))
    .withColumn("quarter", F.quarter("hour_utc"))
    .withColumn("month", F.month("hour_utc"))
    .withColumn("month_name", F.date_format("hour_utc", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("hour_utc"))
    .withColumn("day_of_month", F.dayofmonth("hour_utc"))
    .withColumn("day_of_year", F.dayofyear("hour_utc"))
    # Day of week: ISO numbering (Mon=1, Sun=7) is more intuitive than Spark default
    .withColumn("day_of_week_iso", ((F.dayofweek("hour_utc") + 5) % 7) + 1)
    .withColumn("day_name", F.date_format("hour_utc", "EEEE"))
    # Hour / time attributes
    .withColumn("hour_of_day", F.hour("hour_utc"))
    # Convenience flags
    .withColumn(
        "is_weekend",
        F.col("day_of_week_iso").isin(6, 7),
    )
    .withColumn(
        "is_business_hour_uk",
        (~F.col("is_weekend")) & F.col("hour_of_day").between(7, 17),
    )
    # Day-night flag for solar analysis (approximate, UTC-centric: 06-18 UTC
    # covers daytime in continental Europe with some seasonal slop).
    .withColumn(
        "is_daytime_approx",
        F.col("hour_of_day").between(6, 17),
    )
    # Surrogate integer key (yyyyMMddHH) for fact joins; integer joins are
    # cheaper than timestamp joins on large fact tables.
    .withColumn(
        "time_key",
        F.expr("CAST(date_format(hour_utc, 'yyyyMMddHH') AS BIGINT)"),
    )
    .select(
        "time_key",
        "hour_utc",
        "date_utc",
        "year",
        "quarter",
        "month",
        "month_name",
        "week_of_year",
        "day_of_month",
        "day_of_year",
        "day_of_week_iso",
        "day_name",
        "hour_of_day",
        "is_weekend",
        "is_business_hour_uk",
        "is_daytime_approx",
    )
)

# COMMAND ----------

(
    enriched.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

spark.sql(f"""
    SELECT
      COUNT(*) AS row_count,
      MIN(hour_utc) AS earliest,
      MAX(hour_utc) AS latest,
      COUNT(DISTINCT date_utc) AS distinct_days,
      SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_hours,
      SUM(CASE WHEN is_business_hour_uk THEN 1 ELSE 0 END) AS uk_business_hours,
      SUM(CASE WHEN is_daytime_approx THEN 1 ELSE 0 END) AS daytime_hours
    FROM {TARGET_TABLE}
""").show(truncate=False)
