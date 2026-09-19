# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
"""Etapa 6: calcula prioridade explicável e cria fila Gold de alertas."""

from pyspark.sql import functions as F


def parameter(name, default, label):
    try:
        dbutils.widgets.text(name, default, label)  # type: ignore[name-defined]
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


CATALOG = parameter("catalog", "workspace", "Catálogo")
CORE_SCHEMA = parameter("schema_core", "risksense_core", "Schema core")
BRONZE_SCHEMA = parameter("schema_bronze", "risksense_bronze", "Schema Bronze")
SILVER_SCHEMA = parameter("schema_silver", "risksense_silver", "Schema Silver")
GOLD_SCHEMA = parameter("schema_gold", "risksense_gold", "Schema Gold")
CORE, BRONZE = f"{CATALOG}.{CORE_SCHEMA}", f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER, GOLD = f"{CATALOG}.{SILVER_SCHEMA}", f"{CATALOG}.{GOLD_SCHEMA}"
TARGET = f"{GOLD}.risk_alerts"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

matches = spark.table(f"{SILVER}.account_event_matches")
documents = spark.table(f"{BRONZE}.raw_documents").select(
    "document_id", "source_id", "title", "url", "published_at")
sources = spark.table(f"{CORE}.external_source_config").select("source_id", "reliability_weight")

severity = F.create_map([F.lit(x) for x in ["LOW", .20, "MEDIUM", .50, "HIGH", .80, "CRITICAL", 1.0]])
event_weight = F.create_map([F.lit(x) for x in [
    "FINANCIAL_DISTRESS", 1.0, "REGULATORY_LEGAL", .85, "CORPORATE_GOVERNANCE", .60,
    "OPERATIONAL_DISRUPTION", .80, "REPUTATIONAL_INTEGRITY", .75]])

base = matches.join(documents, "document_id", "left").join(sources, "source_id", "left")
max_exposure = base.agg(F.max("financial_exposure")).first()[0] or 1.0
signals = (base
    .withColumn("source_score", F.coalesce("reliability_weight", F.lit(.50)))
    .withColumn("entity_score", F.col("match_confidence"))
    .withColumn("event_score", event_weight[F.col("event_type")] * severity[F.col("severity")])
    .withColumn("exposure_score", F.least(F.lit(1.0), F.col("financial_exposure") / F.lit(max_exposure)))
    .withColumn("internal_score", F.least(F.lit(1.0),
        F.when(F.col("tpv_trend_90d") < 0, -F.col("tpv_trend_90d") * 2).otherwise(0) +
        F.when(F.col("delinquency_flag"), .35).otherwise(0) +
        F.when(F.col("negative_agenda") > 0, .20).otherwise(0) +
        F.least(F.lit(.25), F.col("chargeback_rate") * 10)))
    .withColumn("recency_score", F.greatest(F.lit(.20), F.lit(1.0) - F.datediff(F.current_date(), F.to_date("published_at")) / 30))
    .withColumn("priority_score", F.round(100 * (
        .15 * F.col("source_score") + .15 * F.col("entity_score") + .25 * F.col("event_score") +
        .20 * F.col("exposure_score") + .15 * F.col("internal_score") + .10 * F.col("recency_score")), 1))
    .withColumn("priority_level", F.when(F.col("priority_score") >= 80, "CRITICAL")
        .when(F.col("priority_score") >= 65, "HIGH").when(F.col("priority_score") >= 45, "MEDIUM").otherwise("LOW"))
    .withColumn("alert_id", F.sha2(F.concat_ws("|", "event_id", "account_id"), 256))
    .withColumn("status", F.lit("PENDING_REVIEW"))
    .withColumn("created_at", F.current_timestamp()))

signals.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)
print(f"Etapa 6: {signals.count()} sinais priorizados em {TARGET}")
display(signals.select("alert_id", "account_id", "event_type", "priority_level", "priority_score",
                       "source_score", "entity_score", "event_score", "exposure_score", "internal_score"))
