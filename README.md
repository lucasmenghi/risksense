# RiskSense

Pipeline Databricks para transformar sinais externos em dados estruturados e,
posteriormente, cruzá-los com uma carteira autorizada de Grandes Contas.

> Este repositório público contém somente dados sintéticos. Não publique dados,
> credenciais, nomes de tabelas ou documentos internos da Cielo.

## Implementação atual

1. `databricks/01_generate_large_accounts.py`: carteira sintética.
2. `databricks/02_build_company_master.py`: empresas, aliases e grupos econômicos.
3. `databricks/03_ingest_external_sources.py`: GDELT, Google News RSS,
   RSS/Atom configurável, Querido Diário e CEIS/CNEP/CEPIM.

A etapa 3 não consome fontes CSV. Feeds de RI, reguladores e APIs comerciais
podem ser habilitados em `external_source_config` após validação de endpoint,
licença e credencial.

O Google News RSS fica desabilitado por padrão devido às restrições de uso do
feed. Habilite `enable_news_rss=true` apenas após autorização jurídica/licença.

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
