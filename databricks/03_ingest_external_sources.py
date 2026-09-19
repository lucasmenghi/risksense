# Databricks notebook source
"""Etapa 3: ingere GDELT, CVM/IPE e CEIS/CNEP no contrato Bronze."""

import csv, hashlib, io, json, uuid, zipfile
from datetime import datetime, timezone
import requests
from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T


def parameter(name, default, label):
    try:
        dbutils.widgets.text(name, default, label)  # type: ignore[name-defined]
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


CATALOG = parameter("catalog", "workspace", "Catálogo")
CORE_SCHEMA = parameter("schema_core", "risksense_core", "Schema core")
BRONZE_SCHEMA = parameter("schema_bronze", "risksense_bronze", "Schema Bronze")
ENABLE_GDELT = parameter("enable_gdelt", "true", "Executar GDELT").lower() == "true"
ENABLE_CVM = parameter("enable_cvm", "true", "Executar CVM").lower() == "true"
ENABLE_TRANSPARENCY = parameter("enable_transparency", "false", "Executar CEIS/CNEP").lower() == "true"
MAX_COMPANIES = int(parameter("max_companies", "20", "Máximo de empresas"))
MAX_GDELT_RECORDS = int(parameter("max_gdelt_records", "25", "Resultados GDELT/empresa"))
CVM_YEAR = int(parameter("cvm_year", str(datetime.now().year), "Ano CVM/IPE"))
SECRET_SCOPE = parameter("transparency_secret_scope", "risksense", "Secret scope")
SECRET_KEY = parameter("transparency_secret_key", "portal_transparencia_token", "Secret key")

CORE, BRONZE = f"{CATALOG}.{CORE_SCHEMA}", f"{CATALOG}.{BRONZE_SCHEMA}"
RAW_TABLE, RUN_TABLE = f"{BRONZE}.raw_documents", f"{BRONZE}.ingestion_runs"
RUN_ID, COLLECTED_AT = str(uuid.uuid4()), datetime.now(timezone.utc)
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "RiskSense/0.1 (data-research)"})
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CORE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")

RAW_SCHEMA = T.StructType([
    T.StructField("document_id", T.StringType(), False),
    T.StructField("source_id", T.StringType(), False),
    T.StructField("source_type", T.StringType(), False),
    T.StructField("external_id", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("raw_text", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("author", T.StringType()),
    T.StructField("published_at", T.TimestampType()),
    T.StructField("collected_at", T.TimestampType(), False),
    T.StructField("language", T.StringType()),
    T.StructField("content_type", T.StringType(), False),
    T.StructField("raw_payload", T.StringType(), False),
    T.StructField("content_hash", T.StringType(), False),
    T.StructField("query_company_id", T.StringType()),
    T.StructField("query_alias", T.StringType()),
    T.StructField("ingestion_run_id", T.StringType(), False),
])
RUN_SCHEMA = "ingestion_run_id string, source_id string, started_at timestamp, finished_at timestamp, status string, records_collected long, error_message string"


def parse_date(value):
    if not value:
        return None
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def document(source_id, source_type, payload, external_id=None, title=None, raw_text=None,
             url=None, author=None, published_at=None, language=None,
             content_type="application/json", company_id=None, query_alias=None):
    raw_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    identity = "|".join([source_id, str(external_id or ""), str(url or ""), str(title or ""), str(raw_text or "")])
    digest = hashlib.sha256(identity.encode()).hexdigest()
    return (f"{source_id}:{digest}", source_id, source_type, external_id, title, raw_text,
            url, author, published_at, COLLECTED_AT, language, content_type, raw_payload,
            digest, company_id, query_alias, RUN_ID)


def get_json(url, params=None, headers=None):
    response = HTTP.get(url, params=params, headers=headers, timeout=(10, 60))
    response.raise_for_status()
    return response.json()


def collect_gdelt(companies):
    records = []
    endpoint = "https://api.gdeltproject.org/api/v2/doc/doc"
    for company in companies:
        alias = company["trade_name"] or company["legal_name"]
        data = get_json(endpoint, {"query": f'"{alias}" sourcecountry:brazil', "mode": "ArtList",
                                   "maxrecords": MAX_GDELT_RECORDS, "format": "json", "sort": "DateDesc"})
        for item in data.get("articles", []):
            records.append(document("GDELT_DOC", "NEWS_AGGREGATOR", item,
                item.get("url"), item.get("title"), item.get("title"), item.get("url"),
                item.get("domain"), parse_date(item.get("seendate")), item.get("language"),
                company_id=company["company_id"], query_alias=alias))
    return records


def collect_cvm():
    url = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/IPE/DADOS/ipe_cia_aberta_{CVM_YEAR}.zip"
    response = HTTP.get(url, timeout=(10, 180))
    response.raise_for_status()
    records = []
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        csv_files = [x for x in archive.namelist() if x.lower().endswith(".csv")]
        if not csv_files:
            raise ValueError("ZIP CVM sem arquivo CSV")
        with archive.open(csv_files[0]) as raw:
            reader = csv.DictReader(io.TextIOWrapper(raw, encoding="latin-1"), delimiter=";")
            for item in reader:
                category = (item.get("Categoria") or "").upper()
                if not any(x in category for x in ("FATO RELEVANTE", "COMUNICADO", "ASSEMBLEIA", "AVISO")):
                    continue
                link = item.get("Link_Download") or item.get("Link_Documento")
                ext_id = item.get("Protocolo_Entrega") or item.get("Protocolo")
                title = " - ".join(filter(None, [item.get("Nome_Companhia"), item.get("Categoria"), item.get("Tipo")]))
                records.append(document("CVM_IPE", "REGULATORY_FILING", item, ext_id, title,
                    item.get("Assunto") or item.get("Tipo"), link, item.get("Nome_Companhia"),
                    parse_date(item.get("Data_Entrega")), "Portuguese", "text/csv"))
    return records


def transparency_token():
    try:
        return dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)  # type: ignore[name-defined]
    except Exception as error:
        print(f"CEIS/CNEP ignorado: secret indisponível ({type(error).__name__}).")
        return None


def collect_transparency(companies, token):
    records, base = [], "https://api.portaldatransparencia.gov.br/api-de-dados"
    for company in companies:
        if company["is_synthetic"]:
            continue
        for dataset in ("ceis", "cnep"):
            for page in range(1, 101):
                data = get_json(f"{base}/{dataset}", {"codigoSancionado": company["cnpj"], "pagina": page},
                                {"chave-api-dados": token})
                if not data:
                    break
                for item in data:
                    ext_id = str(item.get("id") or item.get("numeroProcesso") or "")
                    records.append(document(f"TRANSPARENCIA_{dataset.upper()}", "GOVERNMENT_SANCTION",
                        item, ext_id, f"Registro {dataset.upper()} - {company['legal_name']}",
                        json.dumps(item, ensure_ascii=False, default=str),
                        "https://portaldatransparencia.gov.br/sancoes", published_at=None,
                        language="Portuguese", company_id=company["company_id"], query_alias=company["cnpj"]))
    return records


def upsert(records):
    if not records:
        return 0
    frame = spark.createDataFrame(records, RAW_SCHEMA).dropDuplicates(["document_id"])
    if not spark.catalog.tableExists(RAW_TABLE):
        frame.write.format("delta").mode("overwrite").saveAsTable(RAW_TABLE)
    else:
        (DeltaTable.forName(spark, RAW_TABLE).alias("t")
         .merge(frame.alias("s"), "t.document_id = s.document_id")
         .whenNotMatchedInsertAll().execute())
    return frame.count()


def log_run(source, started, status, count, error=None):
    spark.createDataFrame([(RUN_ID, source, started, datetime.now(timezone.utc), status, count, error)], RUN_SCHEMA) \
        .write.format("delta").mode("append").saveAsTable(RUN_TABLE)


registry = [
    ("GDELT_DOC", "NEWS_AGGREGATOR", "API", "https://api.gdeltproject.org/", "PUBLIC", 0.60, True),
    ("CVM_IPE", "REGULATORY_FILING", "BULK_ZIP", "https://dados.cvm.gov.br/", "PUBLIC", 0.95, True),
    ("TRANSPARENCIA_CEIS", "GOVERNMENT_SANCTION", "API", "https://api.portaldatransparencia.gov.br/", "PUBLIC_TOKEN", 0.95, True),
    ("TRANSPARENCIA_CNEP", "GOVERNMENT_SANCTION", "API", "https://api.portaldatransparencia.gov.br/", "PUBLIC_TOKEN", 0.95, True),
]
(spark.createDataFrame(registry, "source_id string, source_type string, access_method string, base_url string, access_class string, reliability_weight double, active boolean")
 .withColumn("updated_at", F.current_timestamp()).write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(f"{CORE}.source_registry"))

companies = [row.asDict() for row in spark.table(f"{CORE}.companies").limit(MAX_COMPANIES).collect()]
jobs = []
if ENABLE_GDELT:
    jobs.append(("GDELT_DOC", lambda: collect_gdelt(companies)))
if ENABLE_CVM:
    jobs.append(("CVM_IPE", collect_cvm))
if ENABLE_TRANSPARENCY:
    token = transparency_token()
    if token:
        jobs.append(("TRANSPARENCIA_CEIS_CNEP", lambda: collect_transparency(companies, token)))

total = 0
for source, collector in jobs:
    started = datetime.now(timezone.utc)
    try:
        count = upsert(collector())
        total += count
        log_run(source, started, "SUCCESS", count)
        print(f"{source}: {count} documentos")
    except Exception as error:
        message = f"{type(error).__name__}: {str(error)[:1500]}"
        log_run(source, started, "FAILED", 0, message)
        print(f"{source}: {message}")

print(f"Etapa 3: {total} registros processados; run_id={RUN_ID}")
if spark.catalog.tableExists(RAW_TABLE):
    display(spark.table(RAW_TABLE).groupBy("source_id").count().orderBy(F.desc("count")))
