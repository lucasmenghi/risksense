import os, uuid
from datetime import datetime, timezone
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

st.set_page_config(page_title="RiskSense", page_icon="🛡️", layout="wide")
cfg = Config()
CATALOG = os.getenv("RISKSENSE_CATALOG", "workspace")
GOLD_SCHEMA = os.getenv("RISKSENSE_GOLD_SCHEMA", "risksense_gold")
WAREHOUSE_PATH = os.getenv("DATABRICKS_WAREHOUSE_HTTP_PATH", "")
JOB_ID = os.getenv("DATABRICKS_PIPELINE_JOB_ID", "")
QUEUE = f"{CATALOG}.{GOLD_SCHEMA}.review_queue"
REVIEWS = f"{CATALOG}.{GOLD_SCHEMA}.analyst_reviews"


@st.cache_resource
def connection():
    host = cfg.host.replace("https://", "").replace("http://", "")
    return sql.connect(server_hostname=host, http_path=WAREHOUSE_PATH,
                       credentials_provider=lambda: cfg.authenticate,
                       _use_arrow_native_complex_types=False)


def query(statement, parameters=None):
    with connection().cursor() as cursor:
        cursor.execute(statement, parameters=parameters)
        return cursor.fetchall_arrow().to_pandas()


def execute(statement, parameters=None):
    with connection().cursor() as cursor:
        cursor.execute(statement, parameters=parameters)


st.title("RiskSense")
st.caption("Inteligência externa para priorização e revisão humana de sinais de risco")

with st.sidebar:
    st.subheader("Processamento")
    if JOB_ID:
        if st.button("Atualizar sinais", use_container_width=True, type="primary"):
            run = WorkspaceClient().jobs.run_now(job_id=int(JOB_ID))
            st.success(f"Execução iniciada: {run.response.run_id}")
    else:
        st.info("Configure DATABRICKS_PIPELINE_JOB_ID para executar o pipeline pela aplicação.")
    if st.button("Recarregar dados", use_container_width=True):
        st.cache_data.clear(); st.rerun()

if not WAREHOUSE_PATH:
    st.error("Configure DATABRICKS_WAREHOUSE_HTTP_PATH no app.yaml ou como recurso da aplicação.")
    st.stop()

try:
    data = query(f"""SELECT alert_id, priority_level, priority_score, event_type, severity,
        factual_summary, mentioned_company, account_id, sector, financial_exposure,
        source_id, title, url, match_confidence, status, decision, analyst_comment
        FROM {QUEUE} ORDER BY priority_score DESC LIMIT 500""")
except Exception as error:
    st.error(f"Não foi possível carregar a fila: {error}")
    st.stop()

pending = data[data["decision"].isna()] if not data.empty else data
c1, c2, c3, c4 = st.columns(4)
c1.metric("Sinais", len(data))
c2.metric("Pendentes", len(pending))
c3.metric("Alta prioridade", int(data["priority_level"].isin(["HIGH", "CRITICAL"]).sum()) if not data.empty else 0)
c4.metric("Exposição na fila", f"R$ {pending['financial_exposure'].sum()/1e6:,.1f} mi" if not pending.empty else "R$ 0")

tab1, tab2 = st.tabs(["Fila de análise", "Indicadores"])
with tab1:
    levels = st.multiselect("Prioridade", ["CRITICAL", "HIGH", "MEDIUM", "LOW"], default=["CRITICAL", "HIGH", "MEDIUM"])
    queue = pending[pending["priority_level"].isin(levels)] if levels else pending
    if queue.empty:
        st.info("Não existem sinais pendentes para o filtro selecionado.")
    else:
        selected = st.selectbox("Selecione um sinal", queue.index,
            format_func=lambda i: f"{queue.loc[i, 'priority_level']} · {queue.loc[i, 'mentioned_company']} · {queue.loc[i, 'event_type']}")
        row = queue.loc[selected]
        left, right = st.columns([2, 1])
        with left:
            st.subheader(row["title"] or "Sinal externo")
            st.write(row["factual_summary"])
            if row["url"]: st.link_button("Abrir evidência original", row["url"])
        with right:
            st.metric("Score", row["priority_score"])
            st.write(f"**Empresa:** {row['mentioned_company']}")
            st.write(f"**Conta:** {row['account_id']}")
            st.write(f"**Confiança do vínculo:** {row['match_confidence']:.0%}")
            st.write(f"**Exposição:** R$ {row['financial_exposure']:,.2f}")
        with st.form("review_form"):
            decision = st.radio("Validação", ["RELEVANT", "IRRELEVANT", "WRONG_COMPANY", "DUPLICATE"], horizontal=True)
            comment = st.text_area("Comentário do analista")
            if st.form_submit_button("Salvar validação", type="primary"):
                execute(f"INSERT INTO {REVIEWS} VALUES (?, ?, ?, ?, ?, ?, ?)", [
                    str(uuid.uuid4()), row["alert_id"], decision, comment,
                    os.getenv("DATABRICKS_APP_NAME", "app-user"), datetime.now(timezone.utc), "v1"])
                st.success("Validação registrada."); st.rerun()

with tab2:
    if data.empty:
        st.info("Sem dados para exibir.")
    else:
        st.subheader("Sinais por prioridade")
        st.bar_chart(data.groupby("priority_level").size())
        st.subheader("Sinais por evento")
        st.bar_chart(data.groupby("event_type").size())
        st.subheader("Últimas validações")
        st.dataframe(data[data["decision"].notna()][["mentioned_company", "event_type", "decision", "analyst_comment"]], hide_index=True)

