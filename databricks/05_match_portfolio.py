# Databricks notebook source
"""Etapa 5: resolve empresas citadas e cruza eventos com Grandes Contas."""

import re, unicodedata
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window


def parameter(name, default, label):
    try:
        dbutils.widgets.text(name, default, label)  # type: ignore[name-defined]
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


def normalize(value):
    value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode().upper()
    value = re.sub(r"\b(SA|S A|LTDA|ME|EPP)\b", " ", value)
    return re.sub(r"[^A-Z0-9]+", " ", value).strip()


CATALOG = parameter("catalog", "workspace", "Catálogo")
CORE_SCHEMA = parameter("schema_core", "risksense_core", "Schema core")
SILVER_SCHEMA = parameter("schema_silver", "risksense_silver", "Schema Silver")
CORE, SILVER = f"{CATALOG}.{CORE_SCHEMA}", f"{CATALOG}.{SILVER_SCHEMA}"
TARGET = f"{SILVER}.account_event_matches"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
normalize_udf = F.udf(normalize, T.StringType())

events = (spark.table(f"{SILVER}.external_events").filter("is_relevant AND event_type <> 'NONE'")
          .withColumn("mentioned_company", F.explode_outer("company_names"))
          .withColumn("normalized_mention", normalize_udf("mentioned_company")))
aliases = spark.table(f"{CORE}.company_aliases").filter("alias_type <> 'DOMAIN'")

candidates = (events.alias("e").crossJoin(F.broadcast(aliases.alias("a")))
    .withColumn("exact_match", F.col("e.normalized_mention") == F.col("a.normalized_alias"))
    .withColumn("contains_match", (F.length("a.normalized_alias") >= 5) &
        (F.instr(F.col("e.normalized_mention"), F.col("a.normalized_alias")) > 0))
    .withColumn("edit_similarity", 1 - F.levenshtein("e.normalized_mention", "a.normalized_alias") /
        F.greatest(F.length("e.normalized_mention"), F.length("a.normalized_alias"), F.lit(1)))
    .withColumn("match_confidence", F.when("exact_match", 1.0).when("contains_match", .92)
        .otherwise(F.col("edit_similarity") * .85))
    .filter(F.col("match_confidence") >= .72))

window = Window.partitionBy("event_id", "mentioned_company").orderBy(F.desc("match_confidence"), F.desc("a.confidence"))
best = (candidates.withColumn("candidate_rank", F.row_number().over(window)).filter("candidate_rank = 1")
    .select("e.*", F.col("a.company_id").alias("matched_company_id"), "mentioned_company",
            "normalized_mention", "match_confidence", "alias", "alias_type"))

accounts = spark.table(f"{CORE}.large_accounts").select(
    "account_id", "company_id", "economic_group_id", "cnpj", "sector", "tpv_30d", "tpv_trend_90d",
    "financial_exposure", "chargeback_rate", "negative_agenda", "delinquency_flag")
matched = (best.join(accounts, best.matched_company_id == accounts.company_id, "inner")
    .drop(accounts.company_id).withColumn("matched_at", F.current_timestamp()))

matched.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)
print(f"Etapa 5: {matched.count()} correspondências em {TARGET}")
display(matched.select("event_id", "mentioned_company", "account_id", "match_confidence", "financial_exposure"))
