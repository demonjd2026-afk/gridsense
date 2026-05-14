# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - dim_fuel_type
# MAGIC
# MAGIC Unified fuel-type dimension joining two upstream taxonomies:
# MAGIC
# MAGIC 1. ENTSO-E PsrType codes (B01-B25) used in silver.generation
# MAGIC 2. UK Carbon Intensity API fuel labels (nuclear/solar/wind/...)
# MAGIC    used in silver.carbon_intensity.generation_mix
# MAGIC
# MAGIC Each row carries renewable/low-carbon flags AND a typical lifecycle
# MAGIC carbon-intensity estimate (gCO2-eq per kWh), sourced from IPCC AR5
# MAGIC median values. These are STATIC, lifecycle-cradle-to-grave estimates;
# MAGIC for live grid carbon intensity, query silver.carbon_intensity instead.
# MAGIC
# MAGIC The estimates make queries like
# MAGIC   "what was the carbon-weighted generation for FR last hour?"
# MAGIC clean SQL joins instead of CASE-WHEN ladders.
# MAGIC
# MAGIC References:
# MAGIC - IPCC AR5 WG3 Annex III Table A.III.2 (Schlomer et al. 2014)
# MAGIC - https://www.ipcc.ch/site/assets/uploads/2018/02/ipcc_wg3_ar5_annex-iii.pdf
# MAGIC
# MAGIC Overwrite-each-run.

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

TARGET_TABLE = f"{catalog}.gold.dim_fuel_type"
print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# Schema: (fuel_key, source_taxonomy, source_code, display_name,
#          fuel_category, is_renewable, is_low_carbon, typical_gco2_per_kwh)
#
# fuel_key is OUR canonical key (downstream facts join here).
# source_taxonomy: 'entsoe' or 'uk_ci' - which upstream emits this code
# source_code: the upstream-native code (B14 / "nuclear" / etc.)
# fuel_category: high-level bucket (fossil/nuclear/renewable/storage/other)
#
# IPCC AR5 lifecycle medians (gCO2-eq/kWh):
#   Coal       820    Gas CC      490    Oil         650
#   Biomass    230    Nuclear      12    Hydro        24
#   Solar PV    48    Wind onshore 11    Wind off     12
#   Geothermal  38

FUELS = [
    # --- ENTSO-E taxonomy (PsrType codes) ---
    ("entsoe_b01_biomass", "entsoe", "B01", "Biomass", "renewable", True, False, 230),
    (
        "entsoe_b02_lignite",
        "entsoe",
        "B02",
        "Fossil Brown coal/Lignite",
        "fossil",
        False,
        False,
        1054,
    ),
    (
        "entsoe_b03_coal_gas",
        "entsoe",
        "B03",
        "Fossil Coal-derived gas",
        "fossil",
        False,
        False,
        700,
    ),
    ("entsoe_b04_gas", "entsoe", "B04", "Fossil Gas", "fossil", False, False, 490),
    ("entsoe_b05_hard_coal", "entsoe", "B05", "Fossil Hard coal", "fossil", False, False, 820),
    ("entsoe_b06_oil", "entsoe", "B06", "Fossil Oil", "fossil", False, False, 650),
    ("entsoe_b07_oil_shale", "entsoe", "B07", "Fossil Oil shale", "fossil", False, False, 1000),
    ("entsoe_b08_peat", "entsoe", "B08", "Fossil Peat", "fossil", False, False, 950),
    ("entsoe_b09_geothermal", "entsoe", "B09", "Geothermal", "renewable", True, True, 38),
    ("entsoe_b10_hydro_pumped", "entsoe", "B10", "Hydro Pumped Storage", "storage", True, True, 24),
    (
        "entsoe_b11_hydro_ror",
        "entsoe",
        "B11",
        "Hydro Run-of-river and poundage",
        "renewable",
        True,
        True,
        24,
    ),
    (
        "entsoe_b12_hydro_reservoir",
        "entsoe",
        "B12",
        "Hydro Water Reservoir",
        "renewable",
        True,
        True,
        24,
    ),
    ("entsoe_b13_marine", "entsoe", "B13", "Marine", "renewable", True, True, 20),
    ("entsoe_b14_nuclear", "entsoe", "B14", "Nuclear", "nuclear", False, True, 12),
    ("entsoe_b15_other_renewable", "entsoe", "B15", "Other renewable", "renewable", True, True, 50),
    ("entsoe_b16_solar", "entsoe", "B16", "Solar", "renewable", True, True, 48),
    ("entsoe_b17_waste", "entsoe", "B17", "Waste", "renewable", True, False, 230),
    ("entsoe_b18_wind_offshore", "entsoe", "B18", "Wind Offshore", "renewable", True, True, 12),
    ("entsoe_b19_wind_onshore", "entsoe", "B19", "Wind Onshore", "renewable", True, True, 11),
    ("entsoe_b20_other", "entsoe", "B20", "Other", "other", False, False, 500),
    # --- UK Carbon Intensity taxonomy ---
    ("uk_ci_biomass", "uk_ci", "biomass", "Biomass", "renewable", True, False, 230),
    ("uk_ci_coal", "uk_ci", "coal", "Coal", "fossil", False, False, 820),
    ("uk_ci_imports", "uk_ci", "imports", "Imports", "other", False, False, 350),
    ("uk_ci_gas", "uk_ci", "gas", "Gas", "fossil", False, False, 490),
    ("uk_ci_nuclear", "uk_ci", "nuclear", "Nuclear", "nuclear", False, True, 12),
    ("uk_ci_other", "uk_ci", "other", "Other", "other", False, False, 500),
    ("uk_ci_hydro", "uk_ci", "hydro", "Hydro", "renewable", True, True, 24),
    ("uk_ci_solar", "uk_ci", "solar", "Solar", "renewable", True, True, 48),
    ("uk_ci_wind", "uk_ci", "wind", "Wind", "renewable", True, True, 11),
]

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

schema = StructType(
    [
        StructField("fuel_key", StringType(), False),
        StructField("source_taxonomy", StringType(), False),
        StructField("source_code", StringType(), False),
        StructField("display_name", StringType(), False),
        StructField("fuel_category", StringType(), False),
        StructField("is_renewable", BooleanType(), False),
        StructField("is_low_carbon", BooleanType(), False),
        StructField("typical_gco2_per_kwh", IntegerType(), False),
    ]
)
rows = [
    Row(
        fuel_key=fk,
        source_taxonomy=tx,
        source_code=sc,
        display_name=dn,
        fuel_category=fc,
        is_renewable=ir,
        is_low_carbon=lc,
        typical_gco2_per_kwh=co2,
    )
    for fk, tx, sc, dn, fc, ir, lc, co2 in FUELS
]
df = spark.createDataFrame(rows, schema=schema)

(
    df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Wrote {len(FUELS)} rows to {TARGET_TABLE}")

# COMMAND ----------

spark.sql(f"""
    SELECT
      fuel_category,
      COUNT(*) AS row_count,
      SUM(CASE WHEN is_renewable THEN 1 ELSE 0 END) AS renewables,
      SUM(CASE WHEN is_low_carbon THEN 1 ELSE 0 END) AS low_carbon,
      ROUND(AVG(typical_gco2_per_kwh), 0) AS avg_gco2_per_kwh
    FROM {TARGET_TABLE}
    GROUP BY fuel_category
    ORDER BY avg_gco2_per_kwh DESC
""").show(truncate=False)
