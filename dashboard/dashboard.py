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

    dff["data_coleta"] = pd.to_datetime(dff["data_coleta"])
    dff["data_coleta"] = dff["data_coleta"].dt.floor("D")

    evolucao = (
        dff.groupby("data_coleta")["preco_pix"]
        .mean()
        .reset_index()
        .rename(columns={"preco_pix": "preco_medio"})
    )

    fig = px.line(
        evolucao,
        x="data_coleta",
        y="preco_medio",
        markers=True,
        text="preco_medio",
        labels={
            "data_coleta": "Data",
            "preco_medio": "Preço Médio (R$)"
        },
        template="plotly"
    )

    fig.update_traces(
        line_color="#636EFA",
        texttemplate="R$ %{text:,.0f}",  # formatação
        textposition="top center"
    )

    fig.update_layout(
        xaxis=dict(
            tickformat="%d/%m",
        ),
        yaxis=dict(
            showgrid=False,
            showticklabels=False,
            title=""
        )
    )

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
    frete = dff.groupby("eh_full")["preco_pix"].mean().reset_index()
    frete["label"] = frete["eh_full"].map({True: "Com Full", False: "Sem Full"})
    
    fig4 = px.bar(
        frete, 
        x="label", 
        y="preco_pix",
        text_auto='.2f',
        labels={"label": "Logística", "preco_pix": "Preço Médio (R$)"},
        color="label",
        color_discrete_map={"Com Full": "#00CC96", "Sem Full": "#EF553B"},
        template="plotly_dark"
    )
    
    fig4.update_traces(textposition='outside') # Coloca o texto fora da barra
    fig4.update_layout(showlegend=False, height=350)
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

    top_var = (
        variacao
        .nlargest(10, "variacao")
        .sort_values("variacao", ascending=True)
    )

    top_var["titulo_curto"] = top_var["produto_titulo"].apply(
        lambda x: x[:37] + "..." if len(x) > 40 else x
    )

    top_var["cor"] = ["#FF8C42"] * 9 + ["#D7263D"]

    fig5 = px.bar(
        top_var,
        x="variacao",
        y="titulo_curto",
        orientation="h",
        text="variacao",
        template="plotly"
    )

    fig5.update_traces(
        marker_color="#FF8C42",
        texttemplate="R$ %{text:,.0f}",
        textposition="outside",
    )

    fig5.update_layout(
        height=400,
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis_title="Variação de Preço (R$)",
        yaxis_title="",
        yaxis=dict(
            categoryorder="array",
            categoryarray=top_var["titulo_curto"]
        )
    )

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
        reputacao, 
        x="vendedor_nome", 
        y="reputacao_atual_vendedor",
        labels={"vendedor_nome": "Vendedor", "reputacao_atual_vendedor": "Nota"},
        color="reputacao_atual_vendedor",
        color_continuous_scale="RdYlGn", 
        range_color=[0, 5]
    )
    st.plotly_chart(fig6, use_container_width=True)

st.divider()

col7 = st.columns(1)[0]

with col7:
    st.subheader("8 · Comparação de Preços")

    preco_comp = pd.DataFrame({
        "tipo": ["Pix", "Original", "Parcelado"],
        "valor": [
            dff["preco_pix"].mean(),
            dff["preco_original"].mean(),
            dff["valor_total_parcelado"].mean()
        ]
    })

    fig7 = px.bar(
        preco_comp,
        x="tipo",
        y="valor",
        text="valor",
        labels={"tipo": "Tipo", "valor": "Valor Médio (R$)"},
        template="plotly"
    )

    fig7.update_traces(
        texttemplate="R$ %{text:,.0f}",
        textposition="outside",
        marker_color=["#636EFA", "#FFA15A", "#EF553B"]
    )

    fig7.update_layout(
        showlegend=False,
        yaxis=dict(showgrid=False)
    )

    st.plotly_chart(fig7, use_container_width=True)


st.divider()
st.subheader("9 · Comparação de Preços por Produto")

comp_prod = (
    dff.groupby("produto_titulo")
    .agg(
        preco_pix=("preco_pix", "mean"),
        preco_original=("preco_original", "mean"),
        preco_parcelado=("valor_total_parcelado", "mean")
    )
    .reset_index()
)

top_prod = (
    comp_prod
    .sort_values("preco_pix", ascending=False)
    .head(10)
)

df_melt = top_prod.melt(
    id_vars="produto_titulo",
    value_vars=["preco_pix", "preco_original", "preco_parcelado"],
    var_name="tipo",
    value_name="valor"
)

df_melt["tipo"] = df_melt["tipo"].map({
    "preco_pix": "Pix",
    "preco_original": "Original",
    "preco_parcelado": "Parcelado"
})

df_melt["produto_curto"] = df_melt["produto_titulo"].apply(
    lambda x: x[:30] + "..." if len(x) > 33 else x
)

fig9 = px.bar(
    df_melt,
    x="produto_curto",
    y="valor",
    color="tipo",
    barmode="group",
    text="valor",
    labels={
        "produto_curto": "Produto",
        "valor": "Preço (R$)",
        "tipo": "Tipo de Preço"
    },
    template="plotly"
)

fig9.update_traces(
    texttemplate="R$ %{text:,.0f}",
    textposition="outside"
)

fig9.update_layout(
    xaxis_tickangle=-30,
    yaxis=dict(showgrid=False),
    legend_title=""
)

st.plotly_chart(fig9, use_container_width=True)

st.divider()

# Tabela bruta 
st.subheader("Tabela Bruta de Dados")
cols_show = [
    "data_coleta",
    "timestamp_coleta",

    # produto
    "produto_titulo",
    "nota_no_dia",
    "nota_atual_produto",
    "avaliacoes_no_dia",
    "avaliacoes_atuais_produto",

    # vendedor
    "vendedor_nome",
    "reputacao_no_dia",
    "reputacao_atual_vendedor",
    "entrega_no_dia",
    "entrega_atual_vendedor",
    "vendedor_qtd_vendas_no_dia",
    "vendedor_qtd_vendas_atual",

    # preço
    "preco_pix",
    "preco_original",
    "num_parcelas",
    "valor_parcela",
    "valor_total_parcelado",

    # flags
    "eh_full"
]

st.dataframe(
    dff[cols_show].sort_values("data_coleta", ascending=False).head(500),
    use_container_width=True,
    hide_index=True
)