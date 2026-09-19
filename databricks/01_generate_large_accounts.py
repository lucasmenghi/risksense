# Databricks notebook source
"""Etapa 1: gera uma carteira totalmente sintética de Grandes Contas."""

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window


def parameter(name, default, label):
    try:
        dbutils.widgets.text(name, default, label)  # type: ignore[name-defined]
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


CATALOG = parameter("catalog", "workspace", "Catálogo")
SCHEMA = parameter("schema_core", "risksense_core", "Schema core")
TARGET = f"{CATALOG}.{SCHEMA}.large_accounts"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# CNPJs deliberadamente inválidos para evitar associação com empresas reais.
rows = [
    ("ACC-001", "00000001000100", "CMP-001", "GRP-001", "Varejo", 920e6, 0.04, 84e6, 0.0030, 0.0, False),
    ("ACC-002", "00000002000100", "CMP-002", "GRP-001", "Serviços", 280e6, -0.02, 31e6, 0.0040, 2e6, False),
    ("ACC-003", "00000003000100", "CMP-003", "GRP-002", "Mobilidade", 710e6, -0.11, 96e6, 0.0080, 8.5e6, False),
    ("ACC-004", "00000004000100", "CMP-004", "GRP-003", "Alimentação", 640e6, 0.01, 51e6, 0.0050, 0.0, False),
    ("ACC-005", "00000005000100", "CMP-005", "GRP-004", "Saúde", 390e6, 0.08, 47e6, 0.0020, 0.0, False),
    ("ACC-006", "00000006000100", "CMP-006", "GRP-005", "Educação", 220e6, -0.16, 38e6, 0.0120, 12e6, True),
    ("ACC-007", "00000007000100", "CMP-007", "GRP-006", "Tecnologia", 530e6, 0.12, 42e6, 0.0025, 0.0, False),
    ("ACC-008", "00000008000100", "CMP-008", "GRP-007", "Turismo", 180e6, -0.07, 29e6, 0.0090, 4e6, False),
    ("ACC-009", "00000009000100", "CMP-009", "GRP-008", "Construção", 310e6, -0.13, 73e6, 0.0060, 15e6, True),
    ("ACC-010", "00000010000100", "CMP-010", "GRP-009", "Logística", 470e6, 0.03, 58e6, 0.0045, 0.0, False),
    ("ACC-011", "00000011000100", "CMP-011", "GRP-010", "Energia", 760e6, 0.02, 112e6, 0.0015, 0.0, False),
    ("ACC-012", "00000012000100", "CMP-012", "GRP-011", "Indústria", 350e6, -0.05, 66e6, 0.0055, 3e6, False),
]

schema = "account_id string, cnpj string, company_id string, economic_group_id string, sector string, tpv_30d double, tpv_trend_90d double, financial_exposure double, chargeback_rate double, negative_agenda double, delinquency_flag boolean"
df = (
    spark.createDataFrame(rows, schema)
    .withColumn("cnpj_root", F.substring("cnpj", 1, 8))
    .withColumn("portfolio_segment", F.lit("GRANDES_CONTAS"))
    .withColumn("relationship_owner", F.concat(F.lit("OWNER-"), F.lpad(F.row_number().over(Window.orderBy("account_id")), 3, "0")))
    .withColumn("snapshot_date", F.current_date())
    .withColumn("is_synthetic", F.lit(True))
    .withColumn("created_at", F.current_timestamp())
)

assert df.select("account_id").distinct().count() == df.count()
assert df.filter(~F.col("cnpj").rlike("^[0-9]{14}$")).count() == 0
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET)

print(f"Etapa 1: {df.count()} contas gravadas em {TARGET}")
display(df.orderBy(F.desc("financial_exposure")))
