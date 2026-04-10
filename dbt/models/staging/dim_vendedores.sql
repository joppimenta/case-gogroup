WITH source AS (
    SELECT * FROM {{ source('raw', 'smartphones') }}
),

vendedores_unicos AS (
    SELECT
        REGEXP_EXTRACT(url_produto, r'seller_id=([^&]+)') AS id_vendedor,
        vendedor_nome,
        SAFE_CAST(vendedor_reputacao AS FLOAT64) AS vendedor_reputacao,
        vendedor_qtd_vendas,
        vendedor_desde,
        vendedor_entrega_prazo,
        vendedor_atendimento,
        DATETIME(timestamp_coleta, "America/Sao_Paulo") AS timestamp_coleta,
        ROW_NUMBER() OVER(PARTITION BY REGEXP_EXTRACT(url_produto, r'seller_id=([^&]+)') ORDER BY timestamp_coleta DESC) as row_num
    FROM source
)

SELECT
    id_vendedor,
    vendedor_nome,
    vendedor_reputacao,
    vendedor_qtd_vendas,
    vendedor_desde,
    vendedor_entrega_prazo,
    vendedor_atendimento,
    timestamp_coleta AS ultima_atualizacao
FROM vendedores_unicos
WHERE row_num = 1