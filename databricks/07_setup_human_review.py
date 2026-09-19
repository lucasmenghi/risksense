# Databricks notebook source
"""Etapa 7: cria estrutura de validação humana e visão consolidada."""


def parameter(name, default, label):
    try:
        dbutils.widgets.text(name, default, label)  # type: ignore[name-defined]
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default


CATALOG = parameter("catalog", "workspace", "Catálogo")
GOLD_SCHEMA = parameter("schema_gold", "risksense_gold", "Schema Gold")
GOLD = f"{CATALOG}.{GOLD_SCHEMA}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.analyst_reviews (
  review_id STRING NOT NULL,
  alert_id STRING NOT NULL,
  decision STRING NOT NULL COMMENT 'RELEVANT, IRRELEVANT, WRONG_COMPANY ou DUPLICATE',
  analyst_comment STRING,
  reviewed_by STRING,
  reviewed_at TIMESTAMP NOT NULL,
  model_version STRING,
  CONSTRAINT valid_decision CHECK (decision IN ('RELEVANT','IRRELEVANT','WRONG_COMPANY','DUPLICATE'))
) USING DELTA
""")
spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD}.review_queue AS
SELECT a.*, r.decision, r.analyst_comment, r.reviewed_by, r.reviewed_at
FROM {GOLD}.risk_alerts a
LEFT JOIN (
  SELECT * EXCEPT (rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY alert_id ORDER BY reviewed_at DESC) rn
    FROM {GOLD}.analyst_reviews
  ) WHERE rn = 1
) r USING (alert_id)
""")
print(f"Etapa 7 concluída: {GOLD}.analyst_reviews e {GOLD}.review_queue")
