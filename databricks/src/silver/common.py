# Databricks notebook source
"""Shared helpers for Silver layer jobs.

Silver takes raw envelopes from Bronze, parses+validates them, and writes
two outputs:
  - silver.<source>: only the rows that passed validation, upserted via
    MERGE on a natural key (handles producer-side retries and TSO late
    corrections without duplication).
  - quarantine.<source>: rows that failed parsing or validation, kept for
    audit; reject_reason explains why.

This module centralizes the bits all three Silver jobs share: the Bronze
read, the quarantine write, and the MERGE-upsert primitive. Per-source
notebooks define the parse schema, validation rules, and natural key.
"""

from __future__ import annotations

from collections.abc import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType


def read_bronze(spark: SparkSession, bronze_table_fqn: str) -> DataFrame:
    """Read the latest Bronze snapshot for processing.

    We use batch reads (not streaming) for Silver because the Silver
    transformation involves MERGE semantics that streaming does not
    natively support without foreachBatch. A scheduled batch job that
    reads all of Bronze and upserts into Silver gives us idempotent
    behavior at modest re-processing cost.

    The merge condition skips already-processed rows, so re-reading the
    full Bronze table on each run is cheap after the first pass.
    """
    return spark.table(bronze_table_fqn)


def merge_into_silver(
    spark: SparkSession,
    valid_df: DataFrame,
    silver_table_fqn: str,
    natural_key_cols: list[str],
) -> None:
    """Upsert valid rows into Silver via Delta MERGE.

    Producers publish each logical event multiple times: the UK Carbon
    Intensity API re-emits each settlement period first as a forecast and
    then as an actual 2 hours later; ENTSO-E TSOs back-publish corrections;
    Open-Meteo polls more often than the hourly data refreshes. Bronze is
    append-only, so a given natural key will appear many times.

    Before MERGE we therefore deduplicate the source on natural_key_cols,
    keeping the latest ingested_at row. Without this, Delta MERGE raises
    DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE because it
    cannot decide which source row should update the target.

    On first run (table does not yet exist) we still need to dedupe before
    writing so the table is consistent from row 1.

    Args:
        valid_df: rows that passed validation; must include natural key
            columns and an `ingested_at` timestamp column
        silver_table_fqn: catalog.schema.table for the target Silver table
        natural_key_cols: columns that uniquely identify a logical event
            (e.g. ["region_code", "period_start"])
    """
    # Latest-wins deduplication on the natural key.
    # ROW_NUMBER over a window partitioned by the key, ordered by ingested_at
    # descending, then keep only rn=1.
    from pyspark.sql.window import Window

    window = Window.partitionBy(*natural_key_cols).orderBy(F.col("ingested_at").desc())
    deduped = (
        valid_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    if not spark.catalog.tableExists(silver_table_fqn):
        # First run: create the table from the deduped schema.
        (
            deduped.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(silver_table_fqn)
        )
        return

    target = DeltaTable.forName(spark, silver_table_fqn)
    merge_condition = " AND ".join([f"t.{col} = s.{col}" for col in natural_key_cols])
    (
        target.alias("t")
        .merge(deduped.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_quarantine(
    spark: SparkSession,
    invalid_df: DataFrame,
    quarantine_table_fqn: str,
) -> int:
    """Append invalid rows to the quarantine table.

    invalid_df must already have a reject_reason column. We append (no MERGE)
    because quarantine is meant to be an immutable audit log: each rejection
    instance is its own row even if the same payload is rejected twice.
    """
    row_count = invalid_df.count()
    if row_count == 0:
        return 0

    # Add reject_at_ts so we know when this row was quarantined.
    enriched = invalid_df.withColumn("reject_at_ts", F.current_timestamp())

    (
        enriched.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quarantine_table_fqn)
    )
    return row_count


def split_valid_invalid(
    df: DataFrame,
    validation_predicate: Callable[[DataFrame], DataFrame],
) -> tuple[DataFrame, DataFrame]:
    """Split a parsed DataFrame into (valid, invalid) by a validation rule.

    validation_predicate must add a string column `reject_reason` that is:
      - NULL for rows that pass all checks
      - a short human-readable reason for rows that fail

    We then partition the DataFrame on whether reject_reason is null.
    """
    checked = validation_predicate(df)
    valid = checked.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    invalid = checked.filter(F.col("reject_reason").isNotNull())
    return valid, invalid


def log_counts(
    log,
    *,
    bronze_rows: int,
    parsed_rows: int,
    valid_rows: int,
    invalid_rows: int,
) -> None:
    """Emit a single info log line summarizing the run for the job output."""
    log(
        f"counts bronze={bronze_rows} parsed={parsed_rows} "
        f"valid={valid_rows} invalid={invalid_rows}"
    )
