# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Country Dimension
# MAGIC
# MAGIC A small static mapping table joining country codes to their capital
# MAGIC cities, used as the bridge between Open-Meteo (city grain) and
# MAGIC ENTSO-E (country grain) data in the silver.grid_state join.
# MAGIC
# MAGIC Maintained by hand: 6 rows, one per Big 6 country. Overwritten on
# MAGIC each run since the content is fully determined by the COUNTRIES
# MAGIC constant below - no need for MERGE here.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

TARGET_TABLE = f"{catalog}.silver.country_dim"
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Bridge mapping. The capital_city values MUST exactly match the city names
# in silver.weather (which come from producers/open-meteo/src/main.py
# CITIES). If you add a country, also add a city to the producer.
COUNTRIES = [
    ("GB", "Great Britain", "London"),
    ("FR", "France", "Paris"),
    ("DE", "Germany", "Berlin"),
    ("ES", "Spain", "Madrid"),
    ("IT", "Italy", "Rome"),
    ("NL", "Netherlands", "Amsterdam"),
]

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType

schema = StructType(
    [
        StructField("country_code", StringType(), False),
        StructField("country_name", StringType(), False),
        StructField("capital_city", StringType(), False),
    ]
)
rows = [Row(country_code=c, country_name=n, capital_city=city) for c, n, city in COUNTRIES]
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
