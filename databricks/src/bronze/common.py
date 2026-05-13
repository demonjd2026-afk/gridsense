# Databricks notebook source
"""Shared helpers for Bronze layer streaming jobs.

All three Bronze notebooks (carbon_intensity, open_meteo, entsoe) use the
same Kafka read pattern and the same Delta write target shape; this module
centralizes both so per-topic notebooks stay small and obviously different
only where they should (topic name, table name).

Authentication: Spark Kafka client authenticates to Event Hubs via OAUTHBEARER
using an Azure Service Principal. The SP's client_id, client_secret, and
tenant_id are stored in Azure Key Vault and surfaced into Databricks via the
``gridsense-kv`` KV-backed secret scope.

Why a Service Principal instead of the cluster managed identity (Databricks
Access Connector UAMI)? As of late 2025, Databricks Spark Structured Streaming
does not natively support managed-identity OAuth for Kafka clients - the
documented Microsoft pattern is SP-based. When Databricks adds first-class
Unity Catalog Service Credential support for Kafka (DBR 16.1+, GA pending),
this module can be migrated to use the access connector UAMI and the SP can
be retired.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Secret scope and key names (single source of truth so per-topic notebooks
# do not hard-code these).
SECRET_SCOPE = "gridsense-kv"
SECRET_CLIENT_ID = "databricks-eh-sp-client-id"
SECRET_CLIENT_SECRET = "databricks-eh-sp-secret"
SECRET_TENANT_ID = "databricks-eh-sp-tenant-id"


def _read_sp_credentials(dbutils) -> tuple[str, str, str]:
    """Fetch SP credentials from the gridsense-kv secret scope.

    Returns (client_id, client_secret, tenant_id). The dbutils handle is
    passed in because at module import time we are not inside a notebook
    namespace and have no implicit dbutils; the calling notebook passes
    its own dbutils object.
    """
    client_id = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_CLIENT_ID)
    client_secret = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_CLIENT_SECRET)
    tenant_id = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_TENANT_ID)
    return client_id, client_secret, tenant_id


def eventhubs_kafka_options(
    dbutils,
    bootstrap: str,
    namespace: str,
    topic: str,
    consumer_group: str = "bronze-ingest",
    starting_offsets: str = "earliest",
) -> dict[str, str]:
    """Build the readStream options dict for Event Hubs via Kafka surface.

    Args:
        dbutils: the notebook''s dbutils handle (used to read SP creds).
        bootstrap: e.g. ``evhns-gridsense-dev.servicebus.windows.net:9093``
        namespace: e.g. ``evhns-gridsense-dev`` (no FQDN suffix)
        topic: Event Hubs topic name (matches Kafka "topic")
        consumer_group: Event Hubs consumer group; defaults to ``bronze-ingest``
            which is pre-provisioned by the eventhubs Terraform module for
            every topic.
        starting_offsets: ``earliest`` for an initial backfill, ``latest`` if
            you only care about new events from now on. Use ``earliest`` for
            Bronze: it ensures no data loss if the job has been down.

    The OAUTHBEARER mechanism plus Spark''s built-in OAuth login callback
    handler (kafkashaded.org.apache.kafka...secured.OAuthBearerLoginCallbackHandler)
    obtains a fresh AAD token from the v2 token endpoint on each connection,
    scoped to the EH namespace.
    """
    client_id, client_secret, tenant_id = _read_sp_credentials(dbutils)
    eh_server_fqdn = f"{namespace}.servicebus.windows.net"
    scope = f"https://{eh_server_fqdn}/.default"

    sasl_jaas_config = (
        "kafkashaded.org.apache.kafka.common.security.oauthbearer."
        "OAuthBearerLoginModule required "
        f'clientId="{client_id}" '
        f'clientSecret="{client_secret}" '
        f'scope="{scope}" '
        f'ssl.protocol="SSL";'
    )

    return {
        "kafka.bootstrap.servers": bootstrap,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "OAUTHBEARER",
        "kafka.sasl.jaas.config": sasl_jaas_config,
        "kafka.sasl.oauthbearer.token.endpoint.url": (
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        ),
        "kafka.sasl.login.callback.handler.class": (
            "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured."
            "OAuthBearerLoginCallbackHandler"
        ),
        "kafka.session.timeout.ms": "60000",
        "kafka.request.timeout.ms": "60000",
        "subscribe": topic,
        "startingOffsets": starting_offsets,
        "kafka.group.id": consumer_group,
        "failOnDataLoss": "false",
    }


def read_bronze_stream(
    spark: SparkSession,
    dbutils,
    bootstrap: str,
    namespace: str,
    topic: str,
    consumer_group: str = "bronze-ingest",
) -> DataFrame:
    """Open a streaming DataFrame reading from one Event Hubs topic."""
    opts = eventhubs_kafka_options(dbutils, bootstrap, namespace, topic, consumer_group)
    return spark.readStream.format("kafka").options(**opts).load()


def add_bronze_columns(df: DataFrame) -> DataFrame:
    """Apply the Bronze enrichment that every topic shares.

    From the raw Kafka columns we derive:
      - envelope_json:   the value column decoded as UTF-8 string (preserved
                         intact; downstream Silver does structured parsing)
      - kafka_key:       the producer-supplied partition key (region/city/country)
      - ingested_at_ts:  Spark timestamp at which the micro-batch processed
                         this row (NOT envelope.ingested_at)
      - event_date:      partition column; one Bronze file per day
    """
    return (
        df.withColumn("envelope_json", F.col("value").cast(StringType()))
        .withColumn("kafka_key", F.col("key").cast(StringType()))
        .withColumn("ingested_at_ts", F.current_timestamp())
        .withColumn("event_date", F.to_date(F.col("timestamp")))
        .select(
            "envelope_json",
            "kafka_key",
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            "ingested_at_ts",
            "event_date",
        )
    )


def write_bronze(
    df: DataFrame,
    table_fqn: str,
    checkpoint_path: str,
    trigger_available_now: bool = True,
) -> None:
    """Write the streaming DataFrame to a Bronze Delta table.

    Args:
        df: streaming DataFrame from read_bronze_stream + add_bronze_columns
        table_fqn: fully-qualified ``catalog.schema.table`` name
        checkpoint_path: ADLS path for checkpoint storage. CRITICAL: a unique
            path per stream (per topic) is required for exactly-once semantics.
        trigger_available_now: True for batch-on-schedule (default).
    """
    writer_base = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("event_date")
    )
    if trigger_available_now:
        writer = writer_base.trigger(availableNow=True).toTable(table_fqn)
    else:
        writer = writer_base.toTable(table_fqn)
    writer.awaitTermination()
