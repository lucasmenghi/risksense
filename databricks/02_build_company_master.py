# Databricks notebook source
"""Etapa 2: constrói empresas canônicas, aliases e grupos econômicos."""

import re
import unicodedata
from pyspark.sql import functions as F, types as T


def parameter(name, default, label):
    try:
        dbutils.widgets.text(name, default, label)  # type: ignore[name-defined]
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


def normalize_name(value):
    if not value:
        return value
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode().upper()
    value = re.sub(r"\b(SA|S A|LTDA|ME|EPP)\b", " ", value)
    return re.sub(r"[^A-Z0-9]+", " ", value).strip()


CATALOG = parameter("catalog", "workspace", "Catálogo")
SCHEMA = parameter("schema_core", "risksense_core", "Schema core")
BASE = f"{CATALOG}.{SCHEMA}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BASE}")

seeds = [
    ("CMP-001", "00000001000100", "Aurora Varejo S.A.", "Aurora", "auroravarejo.example", "Varejo", "SP"),
    ("CMP-002", "00000002000100", "Aurora Serviços Digitais Ltda.", "Aurora Pay", "aurorapay.example", "Serviços", "SP"),
    ("CMP-003", "00000003000100", "Vértice Mobilidade S.A.", "Vértice", "verticemob.example", "Mobilidade", "SP"),
    ("CMP-004", "00000004000100", "Mesa Boa Alimentação S.A.", "Mesa Boa", "mesaboa.example", "Alimentação", "SP"),
    ("CMP-005", "00000005000100", "Vida Plena Saúde Ltda.", "Vida Plena", "vidaplena.example", "Saúde", "SP"),
    ("CMP-006", "00000006000100", "Horizonte Educação S.A.", "Horizonte", "horizonteedu.example", "Educação", "MG"),
    ("CMP-007", "00000007000100", "Nuvem Clara Tecnologia S.A.", "Nuvem Clara", "nuvemclara.example", "Tecnologia", "SP"),
    ("CMP-008", "00000008000100", "Rota Azul Turismo Ltda.", "Rota Azul", "rotaazul.example", "Turismo", "RJ"),
    ("CMP-009", "00000009000100", "Pilar Forte Construções S.A.", "Pilar Forte", "pilarforte.example", "Construção", "SP"),
    ("CMP-010", "00000010000100", "Via Norte Logística S.A.", "Via Norte", "vianorte.example", "Logística", "SP"),
    ("CMP-011", "00000011000100", "Sol Nascente Energia S.A.", "Sol Nascente", "solnascente.example", "Energia", "SP"),
    ("CMP-012", "00000012000100", "Metal Sul Indústria S.A.", "Metal Sul", "metalsul.example", "Indústria", "PR"),
]

companies = (
    spark.createDataFrame(seeds, "company_id string, cnpj string, legal_name string, trade_name string, official_domain string, sector string, state string")
    .withColumn("cnpj_root", F.substring("cnpj", 1, 8))
    .withColumn("country", F.lit("BR"))
    .withColumn("company_status", F.lit("ATIVA_SIMULADA"))
    .withColumn("is_synthetic", F.lit(True))
    .withColumn("valid_from", F.current_date())
    .withColumn("valid_to", F.lit(None).cast("date"))
    .withColumn("created_at", F.current_timestamp())
)

normalize_udf = F.udf(normalize_name, T.StringType())
aliases = None
for field, kind in (("legal_name", "LEGAL_NAME"), ("trade_name", "TRADE_NAME"), ("official_domain", "DOMAIN")):
    part = companies.select("company_id", F.col(field).alias("alias")).withColumn("alias_type", F.lit(kind))
    aliases = part if aliases is None else aliases.unionByName(part)
aliases = (
    aliases.withColumn("normalized_alias", normalize_udf("alias"))
    .withColumn("source_id", F.lit("SYNTHETIC_SEED"))
    .withColumn("confidence", F.lit(1.0))
    .withColumn("created_at", F.current_timestamp())
)

groups = (
    spark.table(f"{BASE}.large_accounts").select("economic_group_id", "company_id").distinct()
    .withColumn("relationship_type", F.lit("MEMBER"))
    .withColumn("parent_company_id", F.lit(None).cast("string"))
    .withColumn("source_id", F.lit("SYNTHETIC_SEED"))
    .withColumn("confidence", F.lit(1.0))
    .withColumn("valid_from", F.current_date())
    .withColumn("valid_to", F.lit(None).cast("date"))
    .withColumn("created_at", F.current_timestamp())
)

assert spark.table(f"{BASE}.large_accounts").join(companies, "company_id", "left_anti").count() == 0
assert aliases.filter(F.length("normalized_alias") == 0).count() == 0

for name, df in (("companies", companies), ("company_aliases", aliases), ("economic_group_members", groups)):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{BASE}.{name}")

print(f"Etapa 2: {companies.count()} empresas e {aliases.count()} aliases")
display(companies.orderBy("company_id"))
