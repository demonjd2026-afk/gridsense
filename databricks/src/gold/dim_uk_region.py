# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - dim_uk_region
# MAGIC
# MAGIC UK regions from the UK Carbon Intensity API. 18 rows:
# MAGIC   - 1-14: GB Distribution Network Operator (DNO) regions
# MAGIC   - 15-17: National rollups (England, Scotland, Wales)
# MAGIC   - 18: GB national
# MAGIC
# MAGIC This dim exists because UK carbon intensity is published per DNO
# MAGIC region (not just at country level), which dim_country cannot model.
# MAGIC Joins to fact_carbon_intensity_30min on region_id; rolls up to
# MAGIC dim_country via country_code = "GB".
# MAGIC
# MAGIC The approx_lat/lon columns enable Power BI map visuals in Phase 10
# MAGIC without needing a separate geo lookup.
# MAGIC
# MAGIC Overwrite-each-run: content is fully deterministic from the REGIONS
# MAGIC constant below.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

TARGET_TABLE = f"{catalog}.gold.dim_uk_region"
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# region_id, region_code, region_name, region_type, country_code,
# approx_lat, approx_lon
REGIONS = [
    (1, "North Scotland", "SSEN North Scotland", "DNO", "GB", 57.5, -4.2),
    (2, "South Scotland", "SP Distribution", "DNO", "GB", 55.9, -3.9),
    (3, "North West England", "ENW North West", "DNO", "GB", 53.7, -2.5),
    (4, "North East England", "NPG North East", "DNO", "GB", 54.9, -1.6),
    (5, "Yorkshire", "NPG Yorkshire", "DNO", "GB", 53.8, -1.5),
    (6, "North Wales & Merseyside", "SP Manweb", "DNO", "GB", 53.2, -3.1),
    (7, "South Wales", "WPD South Wales", "DNO", "GB", 51.6, -3.5),
    (8, "West Midlands", "WPD West Midlands", "DNO", "GB", 52.5, -1.9),
    (9, "East Midlands", "WPD East Midlands", "DNO", "GB", 52.9, -1.2),
    (10, "East England", "UKPN East", "DNO", "GB", 52.2, 0.5),
    (11, "South West England", "WPD South West", "DNO", "GB", 50.7, -3.7),
    (12, "South England", "SSEN South", "DNO", "GB", 51.3, -1.2),
    (13, "London", "UKPN London", "DNO", "GB", 51.5, -0.1),
    (14, "South East England", "UKPN South East", "DNO", "GB", 51.1, 0.5),
    (15, "England", "England", "national", "GB", 52.5, -1.5),
    (16, "Scotland", "Scotland", "national", "GB", 56.5, -4.0),
    (17, "Wales", "Wales", "national", "GB", 52.1, -3.8),
    (18, "GB", "GB", "national", "GB", 54.5, -2.5),
]

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

schema = StructType(
    [
        StructField("region_id", IntegerType(), False),
        StructField("region_code", StringType(), False),
        StructField("region_name", StringType(), False),
        StructField("region_type", StringType(), False),
        StructField("country_code", StringType(), False),
        StructField("approx_lat", DoubleType(), False),
        StructField("approx_lon", DoubleType(), False),
    ]
)
rows = [
    Row(
        region_id=rid,
        region_code=code,
        region_name=name,
        region_type=rtype,
        country_code=cc,
        approx_lat=lat,
        approx_lon=lon,
    )
    for rid, code, name, rtype, cc, lat, lon in REGIONS
]
df = spark.createDataFrame(rows, schema=schema)

(
    df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote {len(REGIONS)} rows to {TARGET_TABLE}")

# COMMAND ----------

spark.table(TARGET_TABLE).show(truncate=False)
