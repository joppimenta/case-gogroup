import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import os

st.set_page_config(
    page_title="Monitor de Preços — Smartphones",
    page_icon="📱",
    layout="wide"
)

# ── Conexão BigQuery ──────────────────────────────────────────────────────────

@st.cache_resource
def get_client():
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/credentials/gcp-key.json")
    credentials = service_account.Credentials.from_service_account_file(creds_path)
    return bigquery.Client(credentials=credentials, project="case-gogroup")

@st.cache_data(ttl=3600)
def load_data():
    client = get_client()
    query = """
        SELECT *
        FROM `case-gogroup.datamart.dim_consolidado_smartphones`
    """
    return client.query(query).to_dataframe()


with st.spinner("Carregando dados do BigQuery..."):
    try:
        df = load_data()
    except Exception as e:
        st.error(f"Erro ao conectar ao BigQuery: {e}")
        st.stop()

# ── Header ────────────────────────────────────────────────────────────────────

st.title("📱 Monitor de Preços — Smartphones")
if not df.empty:
    ultima_coleta = df['data_coleta'].max()
    total_registros = len(df)
else:
    ultima_coleta = "N/A"
    total_registros = 0

st.caption(f"Fonte: Magazine Luiza · {ultima_coleta} (última coleta)")
st.divider()


with st.sidebar:
    st.header("Filtros")

    datas = sorted(df["data_coleta"].unique())
    data_min, data_max = st.select_slider(
        "Período",
        options=datas,
        value=(datas[0], datas[-1])
    )

    # Dropdown de Produtos
    lista_produtos = ["Todos"] + sorted(df["produto_titulo"].unique().tolist())
    produto_sel = st.selectbox("Selecionar Produto Específico", lista_produtos)

    # busca produtos por texto
    busca_texto = st.text_input("Ou busque por termo (ex: iPhone, Pro Max)", "")

    # Filtro de Vendedor
    vendedores = ["Todos"] + sorted(df["vendedor_nome"].dropna().unique().tolist())
    vendedor_sel = st.selectbox("Vendedor", vendedores)

    # Filtro de Preço
    preco_min, preco_max = st.slider(
        "Faixa de preço (R$)",
        float(df["preco_pix"].min()),
        float(df["preco_pix"].max()),
        (float(df["preco_pix"].min()), float(df["preco_pix"].max()))
    )

mask = (
    (df["data_coleta"] >= data_min) &
    (df["data_coleta"] <= data_max) &
    (df["preco_pix"] >= preco_min) &
    (df["preco_pix"] <= preco_max)
)

if produto_sel != "Todos":
    mask &= (df["produto_titulo"] == produto_sel)

if busca_texto:
    mask &= df["produto_titulo"].str.contains(busca_texto, case=False, na=False)

if vendedor_sel != "Todos":
    mask &= (df["vendedor_nome"] == vendedor_sel)

dff = df[mask].copy()

if dff.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados")
    st.stop()

st.subheader("Visão Geral")
k1, k2, k3, k4 = st.columns(4)
k1.metric("Preço Médio (Pix)", f"R$ {dff['preco_pix'].mean():,.2f}")
k2.metric("Preço Mínimo",      f"R$ {dff['preco_pix'].min():,.2f}")
k3.metric("Preço Máximo",      f"R$ {dff['preco_pix'].max():,.2f}")
k4.metric("Produtos únicos",   f"{dff['produto_id'].nunique():,}")

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("1 · Evolução do Preço Médio por Dia")
    evolucao = (
        dff.groupby("data_coleta")["preco_pix"]
        .mean()
        .reset_index()
        .rename(columns={"preco_pix": "preco_medio"})
    )
    fig = px.line(
        evolucao, x="data_coleta", y="preco_medio",
        markers=True,
        labels={"data_coleta": "Data", "preco_medio": "Preço Médio (R$)"},
    )
    fig.update_traces(line_color="#636EFA")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("2 · Distribuição por Faixa de Preço (R$ 500)")
    dff["faixa"] = (dff["preco_pix"] // 500 * 500).astype(int)
    dff["faixa_label"] = dff["faixa"].apply(lambda x: f"R$ {x:,} – {x+500:,}")
    faixas = (
        dff.groupby(["faixa", "faixa_label"])
        .size()
        .reset_index(name="qtd")
        .sort_values("faixa")
    )
    fig2 = px.bar(
        faixas, x="faixa_label", y="qtd",
        labels={"faixa_label": "Faixa de Preço", "qtd": "Qtd de Produtos"},
        color="qtd", color_continuous_scale="Blues"
    )
    fig2.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig2, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader("3 · Top 10 Vendedores por Qtd de Ofertas")
    top_vendedores = (
        dff.groupby("vendedor_nome")
        .size()
        .reset_index(name="qtd_anuncios")
        .nlargest(10, "qtd_anuncios")
        .sort_values("qtd_anuncios")
    )
    fig3 = px.bar(
        top_vendedores, x="qtd_anuncios", y="vendedor_nome",
        orientation="h",
        labels={"vendedor_nome": "Vendedor", "qtd_anuncios": "Total de Ofertas"},
        color="qtd_anuncios", color_continuous_scale="Teal"
    )
    fig3.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("4 · Frete Grátis: Preço Médio Comparativo")
    frete = (
        dff.groupby("eh_full")["preco_pix"]
        .mean()
        .reset_index()
    )
    frete["label"] = frete["eh_full"].map({True: "Frete Grátis (Full)", False: "Sem Frete Grátis"})
    fig4 = px.bar(
        frete, x="label", y="preco_pix",
        labels={"label": "", "preco_pix": "Preço Médio (R$)"},
        color="label",
        color_discrete_map={
            "Frete Grátis (Full)": "#00CC96",
            "Sem Frete Grátis":     "#EF553B"
        }
    )
    fig4.update_layout(showlegend=False)
    st.plotly_chart(fig4, use_container_width=True)

col5, col6 = st.columns(2)

with col5:
    st.subheader("5 · Produtos com Maior Variação de Preço")
    variacao = (
        dff.groupby(["produto_id", "produto_titulo"])["preco_pix"]
        .agg(preco_min="min", preco_max="max")
        .reset_index()
    )
    variacao["variacao"] = variacao["preco_max"] - variacao["preco_min"]
    top_var = variacao.nlargest(10, "variacao").sort_values("variacao")
    top_var["titulo_curto"] = top_var["produto_titulo"].str[:40] + "..."
    
    fig5 = px.bar(
        top_var, x="variacao", y="titulo_curto",
        orientation="h",
        labels={"variacao": "Variação (R$)", "titulo_curto": "Produto"},
        color="variacao", color_continuous_scale="Oranges"
    )
    fig5.update_layout(coloraxis_showscale=False)
    st.plotly_chart(fig5, use_container_width=True)

with col6:
    st.subheader("6 · Reputação Atual dos Vendedores")
    reputacao = (
        dff.groupby("vendedor_nome")["reputacao_atual_vendedor"]
        .max()
        .reset_index()
        .nlargest(15, "reputacao_atual_vendedor")
    )
    fig6 = px.bar(
        reputacao, x="vendedor_nome", y="reputacao_atual_vendedor",
        labels={"vendedor_nome": "Vendedor", "reputacao_atual_vendedor": "Nota"},
        color="reputacao_atual_vendedor", color_continuous_scale="Viridis"
    )
    st.plotly_chart(fig6, use_container_width=True)

# Tabela bruta
st.divider()
st.subheader("Tabela Bruta de Dados")
cols_show = [
    "data_coleta", 
    "produto_titulo", 
    "vendedor_nome", 
    "vendedor_qtd_vendas_atual", 
    "reputacao_atual_vendedor",
    "preco_pix", 
    "eh_full"
]

st.dataframe(
    dff[cols_show].sort_values("data_coleta", ascending=False).head(500),
    use_container_width=True,
    hide_index=True
)