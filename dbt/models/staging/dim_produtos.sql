WITH source AS (
    SELECT * FROM {{ source('raw', 'smartphones') }}
),

base_produtos AS (
    SELECT
        REGEXP_EXTRACT(url_produto, r'/p/([^/]+)/') AS produto_id,
        produto_titulo,
        REGEXP_REPLACE(url_produto, r'\?.*$', '') AS url_base,
        
        SAFE_CAST(produto_nota AS FLOAT64) AS produto_nota,
        
        SAFE_CAST(produto_num_avaliacoes AS INT64) AS produto_num_avaliacoes,
        
        DATETIME(timestamp_coleta, "America/Sao_Paulo") AS timestamp_coleta
    FROM source
    WHERE url_produto LIKE '%/p/%'
),

deduplicado AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY produto_id ORDER BY timestamp_coleta DESC) as row_num
    FROM base_produtos
)

SELECT
    produto_id,
    produto_titulo,
    url_base,
    produto_nota,
    produto_num_avaliacoes,
    timestamp_coleta AS ultima_atualizacao
FROM deduplicado
WHERE row_num = 1