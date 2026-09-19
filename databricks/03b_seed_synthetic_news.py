# Databricks notebook source
"""Etapa 3b: cria notícias fictícias para demonstrar o pipeline ponta a ponta."""

import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone

from delta.tables import DeltaTable
from pyspark.sql import types as T


def parameter(name, default, label):
    try:
        dbutils.widgets.text(name, default, label)  # type: ignore[name-defined]
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


CATALOG = parameter("catalog", "workspace", "Catálogo")
CORE_SCHEMA = parameter("schema_core", "risksense_core", "Schema core")
BRONZE_SCHEMA = parameter("schema_bronze", "risksense_bronze", "Schema Bronze")
CORE, BRONZE = f"{CATALOG}.{CORE_SCHEMA}", f"{CATALOG}.{BRONZE_SCHEMA}"
RAW_TABLE = f"{BRONZE}.raw_documents"
CONFIG_TABLE = f"{CORE}.external_source_config"
SOURCE_ID = "SYNTHETIC_NEWS"
RUN_ID = str(uuid.uuid4())
NOW = datetime.now(timezone.utc)

RAW_SCHEMA = T.StructType([
    T.StructField("document_id", T.StringType(), False), T.StructField("source_id", T.StringType(), False),
    T.StructField("source_type", T.StringType(), False), T.StructField("external_id", T.StringType()),
    T.StructField("title", T.StringType()), T.StructField("raw_text", T.StringType()),
    T.StructField("url", T.StringType()), T.StructField("author", T.StringType()),
    T.StructField("published_at", T.TimestampType()), T.StructField("collected_at", T.TimestampType(), False),
    T.StructField("language", T.StringType()), T.StructField("content_type", T.StringType(), False),
    T.StructField("raw_payload", T.StringType(), False), T.StructField("content_hash", T.StringType(), False),
    T.StructField("query_company_id", T.StringType()), T.StructField("query_alias", T.StringType()),
    T.StructField("ingestion_run_id", T.StringType(), False),
])

CONFIG_SCHEMA = "source_id string, source_type string, connector_type string, base_url string, access_class string, reliability_weight double, enabled boolean, requires_secret boolean, secret_key string, notes string"

templates = [
    ("FINANCIAL_DISTRESS", "{name} anuncia renegociação de obrigações financeiras",
     "Em cenário exclusivamente simulado, a {name} informou que iniciou conversas para renegociar obrigações financeiras após pressão de caixa. A companhia afirmou que mantém suas operações e divulgará novas informações ao mercado."),
    ("REGULATORY_LEGAL", "Órgão regulador abre procedimento envolvendo {name}",
     "Em cenário exclusivamente simulado, um órgão regulador abriu procedimento administrativo para solicitar esclarecimentos à {name}. Não há decisão final e a empresa informou que apresentará sua manifestação."),
    ("CORPORATE_GOVERNANCE", "{name} comunica mudança em sua administração",
     "Em cenário exclusivamente simulado, a {name} anunciou mudança relevante em sua diretoria executiva. A transição ocorrerá de forma imediata e um plano de continuidade foi apresentado."),
    ("OPERATIONAL_DISRUPTION", "{name} registra interrupção temporária em operação",
     "Em cenário exclusivamente simulado, a {name} registrou interrupção temporária em uma operação importante. A empresa acionou seu plano de contingência e ainda avalia o impacto financeiro e o prazo de normalização."),
    ("REPUTATIONAL_INTEGRITY", "{name} inicia apuração interna após denúncia pública",
     "Em cenário exclusivamente simulado, a {name} iniciou apuração interna após uma denúncia pública sobre conduta de fornecedores. A companhia declarou que o caso ainda está sob análise e não apresentou conclusão."),
]

companies = [row.asDict() for row in spark.table(f"{CORE}.companies").orderBy("company_id").limit(10).collect()]
if len(companies) < 10:
    raise ValueError("São necessárias ao menos 10 empresas fictícias em risksense_core.companies.")

records = []
for index, company in enumerate(companies):
    event_type, title_template, text_template = templates[index % len(templates)]
    name = company["legal_name"]
    external_id = f"DEMO-{index + 1:03d}"
    title = title_template.format(name=name)
    text = text_template.format(name=name)
    identity = f"{SOURCE_ID}|{external_id}|{company['company_id']}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    payload = {
        "synthetic": True,
        "event_type_expected": event_type,
        "company_id_expected": company["company_id"],
        "notice": "Conteúdo inteiramente fictício, criado apenas para demonstração técnica.",
    }
    records.append((
        f"{SOURCE_ID}:{digest}", SOURCE_ID, "SYNTHETIC_DEMO", external_id,
        title, text, f"https://synthetic.invalid/risksense/{external_id.lower()}",
        "RiskSense Demo", NOW - timedelta(hours=index), NOW, "Portuguese",
        "application/json", json.dumps(payload, ensure_ascii=False, sort_keys=True),
        hashlib.sha256(f"{title}|{text}".encode("utf-8")).hexdigest(),
        company["company_id"], company["trade_name"], RUN_ID,
    ))

frame = spark.createDataFrame(records, RAW_SCHEMA)
if not spark.catalog.tableExists(RAW_TABLE):
    frame.write.format("delta").mode("overwrite").saveAsTable(RAW_TABLE)
else:
    (DeltaTable.forName(spark, RAW_TABLE).alias("target")
     .merge(frame.alias("source"), "target.document_id = source.document_id")
     .whenNotMatchedInsertAll().execute())

source_config = spark.createDataFrame([(
    SOURCE_ID, "SYNTHETIC_DEMO", "GENERATED", "https://synthetic.invalid/risksense",
    "SYNTHETIC", 1.0, True, False, None,
    "Conteúdo fictício para validação do MVP; nunca tratar como evidência real.",
)], CONFIG_SCHEMA)
if not spark.catalog.tableExists(CONFIG_TABLE):
    source_config.write.format("delta").mode("overwrite").saveAsTable(CONFIG_TABLE)
else:
    (DeltaTable.forName(spark, CONFIG_TABLE).alias("target")
     .merge(source_config.alias("source"), "target.source_id = source.source_id")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

print(f"Etapa 3b: {len(records)} notícias fictícias disponíveis em {RAW_TABLE}")
display(frame.select("external_id", "title", "query_company_id", "published_at"))
