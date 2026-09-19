# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
"""Etapa 3: ingestão ampla sem fontes CSV.

Conectores: GDELT, Google News RSS, RSS/Atom configurável, Querido Diário e
Portal da Transparência (CEIS/CNEP/CEPIM). APIs contratadas entram pela tabela
de configuração, sem credenciais no código.
"""

import hashlib, html, json, re, uuid, xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus
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
MAX_COMPANIES = int(parameter("max_companies", "20", "Máximo de empresas"))
MAX_RESULTS = int(parameter("max_results_per_query", "25", "Resultados/consulta"))
LOOKBACK_DAYS = int(parameter("lookback_days", "7", "Janela em dias"))
ENABLE_GDELT = parameter("enable_gdelt", "false", "GDELT (fallback)").lower() == "true"
ENABLE_NEWS_RSS = parameter("enable_news_rss", "false", "Google News RSS (somente se autorizado)").lower() == "true"
ENABLE_CUSTOM_RSS = parameter("enable_custom_rss", "true", "RSS configurável").lower() == "true"
ENABLE_GAZETTES = parameter("enable_gazettes", "false", "Querido Diário (fallback)").lower() == "true"
ENABLE_SANCTIONS = parameter("enable_sanctions", "false", "CEIS/CNEP/CEPIM").lower() == "true"
SECRET_SCOPE = parameter("secret_scope", "risksense", "Secret scope")

CORE, BRONZE = f"{CATALOG}.{CORE_SCHEMA}", f"{CATALOG}.{BRONZE_SCHEMA}"
RAW_TABLE, RUN_TABLE = f"{BRONZE}.raw_documents", f"{BRONZE}.ingestion_runs"
CONFIG_TABLE = f"{CORE}.external_source_config"
RUN_ID, COLLECTED_AT = str(uuid.uuid4()), datetime.now(timezone.utc)
SINCE = (COLLECTED_AT - timedelta(days=LOOKBACK_DAYS)).date().isoformat()
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "RiskSense/0.2 data-research"})
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CORE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")

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
RUN_SCHEMA = "ingestion_run_id string, source_id string, started_at timestamp, finished_at timestamp, status string, records_collected long, error_message string"
CONFIG_SCHEMA = "source_id string, source_type string, connector_type string, base_url string, access_class string, reliability_weight double, enabled boolean, requires_secret boolean, secret_key string, notes string"


def clean(value):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html.unescape(value or ""))).strip()


def parse_date(value):
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def document(source_id, source_type, payload, external_id=None, title=None, text=None,
             url=None, author=None, published=None, language="Portuguese",
             content_type="application/json", company_id=None, alias=None):
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    identity = "|".join(map(str, [source_id, external_id or "", url or "", title or "", text or ""]))
    digest = hashlib.sha256(identity.encode()).hexdigest()
    return (f"{source_id}:{digest}", source_id, source_type, str(external_id or "") or None,
            clean(title), clean(text), url, author, published, COLLECTED_AT, language,
            content_type, raw, digest, company_id, alias, RUN_ID)


def request(url, params=None, headers=None):
    result = HTTP.get(url, params=params, headers=headers, timeout=(10, 90))
    result.raise_for_status()
    return result


def parse_feed(content, source_id, source_type, company_id=None, alias=None):
    try:
        root = ET.fromstring(content)
    except ET.ParseError as first_error:
        # Alguns feeds brasileiros, como UOL Economia, entregam bytes Latin-1
        # sem declarar o encoding no XML. O fallback é restrito a erro de parse.
        try:
            root = ET.fromstring(content.decode("iso-8859-1"))
        except Exception:
            raise first_error
    output = []
    items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")
    for item in items[:MAX_RESULTS]:
        def value(*tags):
            for tag in tags:
                node = item.find(tag)
                if node is not None and node.text:
                    return node.text.strip()
            return None
        title = value("title", "{http://www.w3.org/2005/Atom}title")
        text = value("description", "summary", "{http://www.w3.org/2005/Atom}summary", "{http://www.w3.org/2005/Atom}content")
        link = value("link")
        if not link:
            link_node = item.find("{http://www.w3.org/2005/Atom}link")
            link = link_node.attrib.get("href") if link_node is not None else None
        ext_id = value("guid", "id", "{http://www.w3.org/2005/Atom}id") or link
        date = value("pubDate", "published", "updated", "{http://www.w3.org/2005/Atom}published", "{http://www.w3.org/2005/Atom}updated")
        payload = {"title": title, "description": text, "link": link, "published": date}
        output.append(document(source_id, source_type, payload, ext_id, title, text, link,
                               published=parse_date(date), content_type="application/rss+xml",
                               company_id=company_id, alias=alias))
    return output


def collect_gdelt(companies):
    output, endpoint = [], "https://api.gdeltproject.org/api/v2/doc/doc"
    for company in companies:
        alias = company["trade_name"] or company["legal_name"]
        data = request(endpoint, {"query": f'"{alias}" sourcecountry:brazil', "mode": "ArtList",
            "maxrecords": MAX_RESULTS, "format": "json", "sort": "DateDesc"}).json()
        for item in data.get("articles", []):
            domain = item.get("domain") or ""
            official = domain.endswith("gov.br")
            output.append(document("GDELT_OFFICIAL" if official else "GDELT_NEWS",
                "OFFICIAL_WEB" if official else "NEWS_AGGREGATOR", item, item.get("url"),
                item.get("title"), item.get("title"), item.get("url"), domain,
                parse_date(item.get("seendate")), item.get("language"), company_id=company["company_id"], alias=alias))
    return output


def collect_google_news(companies):
    output = []
    for company in companies:
        alias = company["trade_name"] or company["legal_name"]
        url = f"https://news.google.com/rss/search?q={quote_plus(chr(34)+alias+chr(34))}&hl=pt-BR&gl=BR&ceid=BR:pt-419"
        output.extend(parse_feed(request(url).content, "GOOGLE_NEWS_RSS", "NEWS_SEARCH", company["company_id"], alias))
    return output


def collect_custom_rss(configs):
    output = []
    for cfg in configs:
        if cfg["connector_type"] == "RSS" and cfg["enabled"] and cfg["base_url"]:
            started = datetime.now(timezone.utc)
            try:
                records = parse_feed(request(cfg["base_url"]).content, cfg["source_id"], cfg["source_type"])
                output.extend(records)
                log_run(cfg["source_id"], started, "SUCCESS", len(records))
                print(f"{cfg['source_id']}: {len(records)} itens RSS")
            except Exception as error:
                message = f"{type(error).__name__}: {str(error)[:1500]}"
                log_run(cfg["source_id"], started, "FAILED", 0, message)
                print(f"{cfg['source_id']}: ignorado nesta execução - {message}")
    return output


def collect_gazettes(companies):
    output, endpoint = [], "https://api.queridodiario.ok.org.br/gazettes"
    for company in companies:
        alias = company["trade_name"] or company["legal_name"]
        data = request(endpoint, {"querystring": f'"{alias}"', "published_since": SINCE,
                                  "size": MAX_RESULTS, "offset": 0}).json()
        for item in data.get("gazettes", data.get("results", [])):
            url = item.get("url") or item.get("file_url")
            text = item.get("excerpt") or item.get("text") or item.get("edition")
            output.append(document("QUERIDO_DIARIO", "MUNICIPAL_GAZETTE", item,
                item.get("id") or url, f"Diário oficial - {item.get('territory_name', '')}", text,
                url, item.get("territory_name"), parse_date(item.get("date") or item.get("published_at")),
                company_id=company["company_id"], alias=alias))
    return output


def secret(key):
    try:
        return dbutils.secrets.get(SECRET_SCOPE, key)  # type: ignore[name-defined]
    except Exception:
        return None


def collect_sanctions(companies):
    token = secret("portal_transparencia_token")
    if not token:
        raise RuntimeError("Secret portal_transparencia_token não configurado")
    output, base = [], "https://api.portaldatransparencia.gov.br/api-de-dados"
    for company in companies:
        if company.get("is_synthetic"):
            continue
        for dataset in ("ceis", "cnep", "cepim"):
            for page in range(1, 101):
                data = request(f"{base}/{dataset}", {"codigoSancionado": company["cnpj"], "pagina": page},
                               {"chave-api-dados": token}).json()
                if not data:
                    break
                for item in data:
                    output.append(document(f"TRANSPARENCIA_{dataset.upper()}", "GOVERNMENT_SANCTION",
                        item, item.get("id") or item.get("numeroProcesso"),
                        f"Registro {dataset.upper()} - {company['legal_name']}", json.dumps(item, ensure_ascii=False),
                        "https://portaldatransparencia.gov.br/sancoes", company_id=company["company_id"], alias=company["cnpj"]))
    return output


def upsert(records):
    if not records:
        return 0
    frame = spark.createDataFrame(records, RAW_SCHEMA).dropDuplicates(["document_id"])
    if not spark.catalog.tableExists(RAW_TABLE):
        frame.write.format("delta").mode("overwrite").saveAsTable(RAW_TABLE)
    else:
        (DeltaTable.forName(spark, RAW_TABLE).alias("t").merge(frame.alias("s"), "t.document_id=s.document_id")
         .whenNotMatchedInsertAll().execute())
    return frame.count()


def log_run(source, started, status, count, error=None):
    spark.createDataFrame([(RUN_ID, source, started, datetime.now(timezone.utc), status, count, error)], RUN_SCHEMA) \
        .write.format("delta").mode("append").saveAsTable(RUN_TABLE)


defaults = [
    ("GDELT_NEWS", "NEWS_AGGREGATOR", "JSON_API", "https://api.gdeltproject.org/api/v2/doc/doc", "PUBLIC", .60, True, False, None, "Notícias globais"),
    ("GDELT_OFFICIAL", "OFFICIAL_WEB", "JSON_API", "https://api.gdeltproject.org/api/v2/doc/doc", "PUBLIC", .80, True, False, None, "Domínios gov.br"),
    ("GOOGLE_NEWS_RSS", "NEWS_SEARCH", "DYNAMIC_RSS", "https://news.google.com/rss/search", "RESTRICTED_TERMS", .55, False, False, None, "Uso corporativo exige autorização/licença"),
    ("QUERIDO_DIARIO", "MUNICIPAL_GAZETTE", "JSON_API", "https://api.queridodiario.ok.org.br/gazettes", "CC_BY_4", .85, True, False, None, "Diários municipais"),
    ("TRANSPARENCIA_CEIS", "GOVERNMENT_SANCTION", "JSON_API", "https://api.portaldatransparencia.gov.br/api-de-dados/ceis", "PUBLIC_TOKEN", .95, True, True, "portal_transparencia_token", "CEIS"),
    ("TRANSPARENCIA_CNEP", "GOVERNMENT_SANCTION", "JSON_API", "https://api.portaldatransparencia.gov.br/api-de-dados/cnep", "PUBLIC_TOKEN", .95, True, True, "portal_transparencia_token", "CNEP"),
    ("TRANSPARENCIA_CEPIM", "GOVERNMENT_SANCTION", "JSON_API", "https://api.portaldatransparencia.gov.br/api-de-dados/cepim", "PUBLIC_TOKEN", .95, True, True, "portal_transparencia_token", "CEPIM"),
    ("COMPANY_IR_RSS", "COMPANY_DISCLOSURE", "RSS", None, "SOURCE_DEPENDENT", .90, False, False, None, "Cadastrar feed por empresa"),
    ("REGULATOR_RSS", "REGULATORY_NOTICE", "RSS", None, "SOURCE_DEPENDENT", .90, False, False, None, "CVM/BCB/CADE/agências"),
    ("LICENSED_NEWS_API", "LICENSED_NEWS", "JSON_API", None, "LICENSED", .85, False, True, "licensed_news_api_key", "Provedor contratado"),
    ("LICENSED_LEGAL_API", "LEGAL_EVENT", "JSON_API", None, "LICENSED", .90, False, True, "licensed_legal_api_key", "Provedor jurídico"),
    ("RSS_VALOR", "BUSINESS_NEWS", "RSS", "https://valor.globo.com/rss/valor/", "RSS_METADATA", .85, True, False, None, "Valor Econômico"),
    ("RSS_EXAME", "BUSINESS_NEWS", "RSS", "https://exame.com/feed/", "RSS_METADATA", .80, True, False, None, "Exame"),
    ("RSS_BRAZIL_JOURNAL", "BUSINESS_NEWS", "RSS", "https://braziljournal.com/feed/", "RSS_METADATA", .85, True, False, None, "Brazil Journal"),
    ("RSS_MONEY_TIMES", "BUSINESS_NEWS", "RSS", "https://www.moneytimes.com.br/feed/", "RSS_METADATA", .75, True, False, None, "Money Times"),
    ("RSS_NEOFEED", "BUSINESS_NEWS", "RSS", "https://neofeed.com.br/feed/", "RSS_METADATA", .80, True, False, None, "NeoFeed"),
    ("RSS_INVESTNEWS", "BUSINESS_NEWS", "RSS", "https://investnews.com.br/feed/", "RSS_METADATA", .75, True, False, None, "InvestNews"),
    ("RSS_EINVESTIDOR", "BUSINESS_NEWS", "RSS", "https://einvestidor.estadao.com.br/feed/", "RSS_METADATA", .80, True, False, None, "E-Investidor"),
    ("RSS_FORBES_BR", "BUSINESS_NEWS", "RSS", "https://forbes.com.br/feed/", "RSS_METADATA", .75, True, False, None, "Forbes Brasil"),
    ("RSS_G1_ECONOMIA", "GENERAL_ECONOMY_NEWS", "RSS", "https://g1.globo.com/rss/g1/economia/", "RSS_METADATA", .80, True, False, None, "G1 Economia"),
    ("RSS_UOL_ECONOMIA", "GENERAL_ECONOMY_NEWS", "RSS", "https://rss.uol.com.br/feed/economia.xml", "RSS_METADATA", .75, True, False, None, "UOL Economia"),
    ("RSS_BBC_BRASIL", "GENERAL_NEWS", "RSS", "https://feeds.bbci.co.uk/portuguese/rss.xml", "RSS_METADATA", .85, True, False, None, "BBC News Brasil"),
    ("RSS_PODER360", "POLITICAL_REGULATORY_NEWS", "RSS", "https://www.poder360.com.br/feed/", "RSS_METADATA", .75, True, False, None, "Poder360"),
    ("RSS_CANALTECH", "TECHNOLOGY_NEWS", "RSS", "https://canaltech.com.br/rss/", "RSS_METADATA", .70, True, False, None, "Canaltech"),
    ("RSS_TECNOBLOG", "TECHNOLOGY_NEWS", "RSS", "https://tecnoblog.net/feed/", "RSS_METADATA", .70, True, False, None, "Tecnoblog"),
]
seed_sources = spark.createDataFrame(defaults, CONFIG_SCHEMA)
if not spark.catalog.tableExists(CONFIG_TABLE):
    seed_sources.write.format("delta").mode("overwrite").saveAsTable(CONFIG_TABLE)
else:
    # Adiciona novas fontes sem sobrescrever ajustes feitos no Databricks.
    (DeltaTable.forName(spark, CONFIG_TABLE).alias("target")
     .merge(seed_sources.alias("source"), "target.source_id = source.source_id")
     .whenNotMatchedInsertAll().execute())

companies = [x.asDict() for x in spark.table(f"{CORE}.companies").limit(MAX_COMPANIES).collect()]
configs = [x.asDict() for x in spark.table(CONFIG_TABLE).collect()]
jobs = []
if ENABLE_GDELT: jobs.append(("GDELT", lambda: collect_gdelt(companies)))
if ENABLE_NEWS_RSS: jobs.append(("GOOGLE_NEWS_RSS", lambda: collect_google_news(companies)))
if ENABLE_CUSTOM_RSS: jobs.append(("CUSTOM_RSS", lambda: collect_custom_rss(configs)))
if ENABLE_GAZETTES: jobs.append(("QUERIDO_DIARIO", lambda: collect_gazettes(companies)))
if ENABLE_SANCTIONS: jobs.append(("TRANSPARENCIA_SANCTIONS", lambda: collect_sanctions(companies)))

total = 0
for source, collector in jobs:
    started = datetime.now(timezone.utc)
    try:
        count = upsert(collector()); total += count
        log_run(source, started, "SUCCESS", count)
        print(f"{source}: {count} documentos")
    except Exception as error:
        message = f"{type(error).__name__}: {str(error)[:1500]}"
        log_run(source, started, "FAILED", 0, message)
        print(f"{source}: {message}")

print(f"Etapa 3: {total} registros processados; run_id={RUN_ID}")
if spark.catalog.tableExists(RAW_TABLE):
    display(
        spark.table(RAW_TABLE)
        .filter(F.col("ingestion_run_id") == RUN_ID)
        .groupBy("source_id")
        .count()
        .orderBy(F.desc("count"))
    )

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from risksense_bronze.raw_documents
# MAGIC where source_id <> 'CVM_IPE'