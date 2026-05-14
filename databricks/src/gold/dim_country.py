# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - dim_country
# MAGIC
# MAGIC Country dimension for the Gold layer star schema. One row per country,
# MAGIC extending silver.country_dim with EIC codes (ENTSO-E unique identifier
# MAGIC for the bidding zone) and timezone offsets (useful for time-of-day
# MAGIC analysis - Madrid noon != London noon for solar generation).
# MAGIC
# MAGIC Overwrite-each-run: content is fully deterministic from the COUNTRIES
# MAGIC constant below.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

TARGET_TABLE = f"{catalog}.gold.dim_country"
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# country_code, country_name, capital_city, eic_bidding_zone, iso_alpha3,
# timezone_offset_winter_hours, timezone_offset_summer_hours
COUNTRIES = [
    ("GB", "Great Britain", "London", "10YGB----------A", "GBR", 0, 1),
    ("FR", "France", "Paris", "10YFR-RTE------C", "FRA", 1, 2),
    ("DE", "Germany", "Berlin", "10Y1001A1001A83F", "DEU", 1, 2),
    ("ES", "Spain", "Madrid", "10YES-REE------0", "ESP", 1, 2),
    ("IT", "Italy", "Rome", "10YIT-GRTN-----B", "ITA", 1, 2),
    ("NL", "Netherlands", "Amsterdam", "10YNL----------L", "NLD", 1, 2),
]

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

schema = StructType(
    [
        StructField("country_code", StringType(), False),
        StructField("country_name", StringType(), False),
        StructField("capital_city", StringType(), False),
        StructField("eic_bidding_zone", StringType(), False),
        StructField("iso_alpha3", StringType(), False),
        StructField("timezone_offset_winter_hours", IntegerType(), False),
        StructField("timezone_offset_summer_hours", IntegerType(), False),
    ]
)
rows = [
    Row(
        country_code=c,
        country_name=n,
        capital_city=city,
        eic_bidding_zone=eic,
        iso_alpha3=a3,
        timezone_offset_winter_hours=tzw,
        timezone_offset_summer_hours=tzs,
    )
    for c, n, city, eic, a3, tzw, tzs in COUNTRIES
]
df = spark.createDataFrame(rows, schema=schema)

(
    df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote {len(COUNTRIES)} rows to {TARGET_TABLE}")

# COMMAND ----------

spark.table(TARGET_TABLE).show(truncate=False)
