# Pipeline de Monitoramento de Preços — Smartphones

Pipeline de dados end-to-end para coleta, processamento e análise de preços de smartphones coletados a partir do site da Magazine Luiza.

---

## Resumo da arquitetura/pipeline

```
Magazine Luiza (scraping)
        │
        ▼
  Python Scraper
        │
        ▼
      n8n (fila/webhook)
        │
        ▼
   BigQuery (RAW)
        │
        ▼
   dbt (Staging → Marts)
        │
        ▼
  Dashboard (Streamlit)
        │
  Orquestração: Airflow
```

## Ferramentas e bibliotecas utilizadas

| Camada | Ferramenta |
|---|---|
| Coleta | Python (requests + BeautifulSoup) |
| Fila / Stream | n8n (webhook) |
| Armazenamento | Google BigQuery |
| Transformação | dbt Core + dbt-bigquery |
| Visualização | Streamlit |
| Orquestração | Apache Airflow 2.9.1 |
| Infraestrutura | Docker Compose |

---

## Estrutura do Projeto

```
case-gogroup/
├── airflow/
│   └── dags/                          # DAGs do Airflow
├── collector/
│   └── extract.py                     # Script de scraping
├── consumer/                          # Consumidor da fila n8n → BigQuery
├── dbt/
│   ├── models/
│   │   ├── staging/                   # Limpeza, tipagem e modelagem
│   │   │   ├── dim_produtos.sql
│   │   │   ├── dim_vendedores.sql
│   │   │   └── fct_produtos_diario.sql
│   │   ├── marts/                     # Tabela analítica final
│   │   │   └── dim_consolidado_smartphones.sql
│   │   └── sources.yml
│   ├── dbt_project.yml
│   └── profiles.yml
├── dashboard/
│    └── dashboard.py # Dash analítico com as principais informações da dim_consolidado
├── credentials/                       # Credenciais do Google Cloud (não versionado)
├── workflow.json      # Workflow do n8n para importação
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## Modelagem de Dados

### Camada RAW
Dados brutos inseridos diretamente do scraper, sem transformação. Cada execução acrescenta novos registros, monitorados pela data/horário de coleta (`timestamp_coleta`).

### Camada Staging

**`dim_produtos`** — Dimensão de produtos com deduplicação. Mantém apenas o registro mais recente por `produto_id`, refletindo sempre as informações mais atuais sobre cada produto.

**`dim_vendedores`** — Dimensão de vendedores com deduplicação. Mantém apenas o registro mais recente por `seller_id`. Consolida os dados mais recentes a respeito dos vendedores, como reputação, quantidade de vendas, métricas de entrega e atendimento.

**`fct_produtos_diario`** — Tabela fato com granularidade diária por combinação de `produto_id + vendedor_id + data_coleta`. Para cada combinação, mantém apenas o último registro do dia (`ROW_NUMBER` com `ORDER BY timestamp DESC`). Contém preços (original, pix, valor total parcelado), métricas do produto e do vendedor no momento da coleta.

### Camada Mart

**`dim_consolidado_smartphones`** — Visão analítica que une a tabela fato com as dimensões, expondo tanto os valores históricos (no dia da coleta) quanto os valores atuais do produto e vendedor. Permite comparar a evolução de métricas como nota, avaliações e reputação ao longo do tempo.

### Estratégia de Idempotência

A pipeline implementa idempotência em duas camadas:

- **RAW → Staging (dimensões):** `ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp DESC)` garante que re-execuções não dupliquem registros nas dimensões, sempre prevalecendo o dado mais recente.
- **RAW → Staging (fato diário):** `ROW_NUMBER() OVER(PARTITION BY produto_id, vendedor_id, data ORDER BY timestamp DESC)` garante exatamente um registro por produto/vendedor/dia, usando sempre a última coleta do dia para enviar para a tabela final

---

## Como Rodar

### Pré-requisitos

- Docker e Docker Compose instalados
- Conta no Google Cloud com BigQuery habilitado
- Chave de serviço GCP com permissões de leitura/escrita no BigQuery

### 1. Clone o repositório

```bash
git clone <url-do-repositorio>
cd case-gogroup
```

### 2. Configure as variáveis de ambiente

```bash
cp .env.example .env
```

Edite o `.env` com seus valores:

```env
GCP_PROJECT_ID=seu-projeto-gcp
GCP_DATASET_RAW=raw
GCP_DATASET_STAGING=staging
GCP_DATASET_MARTS=datamart
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/credentials/gcp-key.json
```

### 3. Adicione a chave GCP

Coloque o arquivo JSON da sua conta de serviço em:

```
credentials/gcp-key.json
```

### 4. Suba o ambiente

```bash
docker compose up --build -d
```

### 5. Configure o n8n

1. Acesse [http://localhost:5678](http://localhost:5678)
2. Vá em **Settings → Import workflow**
3. Importe o arquivo `workflow.json` da raiz do projeto
4. Vá em **Credentials** e configure uma nova credencial do tipo **Google Service Account** apontando para o seu `gcp-key.json`
5. Vincule a credencial ao node **Execute a SQL query** do workflow
6. Atualize o campo `projectId` no node com o seu `GCP_PROJECT_ID`
7. Ative o workflow

### 6. Acesse o Airflow

Abra [http://localhost:8080](http://localhost:8080) e ative a DAG `pipeline_magalu_bigquery`.

Credenciais padrão: `airflow / airflow`

---

## Orquestração

A DAG `pipeline_magalu_bigquery` executa diariamente às 18h (horário de Brasília) com as seguintes tasks em sequência:

```
extrair_dados_magalu >> dbt_transform
```

- **extrair_dados_magalu:** executa o scraper Python, coleta listagens de smartphones no Magazine Luiza e envia para o n8n, que persiste no BigQuery (camada RAW).
- **dbt_transform:** executa `dbt run` aplicando todas as transformações de staging e marts.

Configuração de retentativas: 1 retry com intervalo de 5 minutos.

## Acesso ao Dashboard

Abra [http://localhost:8501](http://localhost:8501) e visualize os dados.

## Variáveis de Ambiente

Veja o arquivo `.env.example` para todas as variáveis necessárias.