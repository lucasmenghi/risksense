# RiskSense

Pipeline Databricks para transformar sinais externos em dados estruturados e,
posteriormente, cruzá-los com uma carteira autorizada de Grandes Contas.

> Este repositório público contém somente dados sintéticos. Não publique dados,
> credenciais, nomes de tabelas ou documentos internos da Cielo.

## Implementação atual

1. `databricks/01_generate_large_accounts.py`: carteira sintética.
2. `databricks/02_build_company_master.py`: empresas, aliases e grupos econômicos.
3. `databricks/03_ingest_external_sources.py`: feeds diretos de jornais,
   RSS/Atom configurável, GDELT opcional, Querido Diário opcional e
   CEIS/CNEP/CEPIM.
   `databricks/03b_seed_synthetic_news.py`: dez notícias inteiramente fictícias,
   vinculadas à carteira simulada, para demonstrar o pipeline ponta a ponta.
4. `databricks/04_extract_events_llm.py`: extração estruturada em cinco tipos de evento.
5. `databricks/05_match_portfolio.py`: resolução de empresa e cruzamento com a carteira.
6. `databricks/06_prioritize_signals.py`: score explicável e tabela Gold.
7. `databricks/07_setup_human_review.py`: feedback humano e fila consolidada.

A etapa 3 não consome fontes CSV. Feeds de RI, reguladores e APIs comerciais
podem ser habilitados em `external_source_config` após validação de endpoint,
licença e credencial.

O Google News RSS fica desabilitado por padrão devido às restrições de uso do
feed. Habilite `enable_news_rss=true` apenas após autorização jurídica/licença.

O conjunto padrão prioriza feeds diretos: Valor Econômico, Exame, Brazil
Journal, Money Times, NeoFeed, InvestNews, E-Investidor, Forbes Brasil, G1
Economia, UOL Economia, BBC News Brasil, Poder360, Canaltech e Tecnoblog.
GDELT e Querido Diário ficam desabilitados por padrão e funcionam como fallback.
Cada feed é processado e auditado isoladamente. Feeds Latin-1 sem declaração de
encoding, como o UOL Economia, recebem fallback controlado para ISO-8859-1.

Para executar a demonstração completa, rode a etapa `03b` depois da ingestão e
antes da etapa 4. A fonte `SYNTHETIC_NEWS` é identificada como simulação em seu
payload, configuração e URL, e não deve ser tratada como evidência real.

## Taxonomia inicial

- `FINANCIAL_DISTRESS`: deterioração, reestruturação, recuperação ou falência.
- `REGULATORY_LEGAL`: investigação, processo material, sanção ou regulação.
- `CORPORATE_GOVERNANCE`: controle, M&A, administração ou estrutura societária.
- `OPERATIONAL_DISRUPTION`: paralisação, ruptura operacional, logística ou cibernética.
- `REPUTATIONAL_INTEGRITY`: fraude, corrupção, conduta ou crise reputacional.

## Aplicação

A pasta `app/` contém uma aplicação Streamlit para Databricks Apps. Ela lê
`risksense_gold.review_queue`, apresenta os sinais priorizados e grava as
decisões `RELEVANT`, `IRRELEVANT`, `WRONG_COMPANY` ou `DUPLICATE` em
`risksense_gold.analyst_reviews`.

Antes do deploy:

1. Configure um endpoint de Model Serving no widget `model_endpoint` da etapa 4.
2. Crie um Job sob demanda encadeando as etapas 3 a 7.
3. Informe no `app/app.yaml` o HTTP Path do SQL Warehouse e o ID do Job.
4. Conceda ao service principal da aplicação `CAN USE` no warehouse, `SELECT`
   nas tabelas de leitura e `MODIFY` em `analyst_reviews`.

Execute os scripts nessa ordem. Todos possuem widgets e usam, por padrão:

```text
catalog = workspace
schema_core = risksense_core
schema_bronze = risksense_bronze
```

Para CEIS/CNEP, salve o token do Portal da Transparência em um Databricks
Secret Scope e informe `transparency_secret_scope` e
`transparency_secret_key`. Nunca coloque o token no código.

Os CNPJs sintéticos são deliberadamente inválidos. Para consultas por CNPJ,
substitua `large_accounts` por uma visão autorizada no ambiente corporativo e
execute a ingestão com `enable_transparency=true`.

## Tabelas

| Tabela | Conteúdo |
|---|---|
| `risksense_core.large_accounts` | carteira e indicadores simulados |
| `risksense_core.companies` | identidade canônica das empresas |
| `risksense_core.company_aliases` | nomes para resolução de entidades |
| `risksense_core.economic_group_members` | relações empresa-grupo |
| `risksense_core.source_registry` | catálogo das fontes |
| `risksense_bronze.raw_documents` | contrato Bronze comum |
| `risksense_bronze.ingestion_runs` | auditoria das execuções |

Valide licenciamento, retenção e limites de cada fonte antes do uso produtivo.
