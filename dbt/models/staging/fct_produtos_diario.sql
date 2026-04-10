WITH source AS (
    SELECT * FROM {{ source('raw', 'smartphones') }}
),

deduplicado_diario AS (
    SELECT
        REGEXP_EXTRACT(url_produto, r'/p/([^/]+)/') AS produto_id,
        REGEXP_EXTRACT(url_produto, r'seller_id=([^&]+)') AS vendedor_id,
        DATE(DATETIME(timestamp_coleta, "America/Sao_Paulo")) AS data_coleta,
        DATETIME(timestamp_coleta, "America/Sao_Paulo") AS timestamp_coleta,
        
        -- limpeza dos valores dos preços
        SAFE_CAST(
            REPLACE(
                REPLACE(
                REGEXP_REPLACE(preco_original, r'[^0-9,\.]', ''),
                '.', ''
                ),
                ',', '.'
            ) AS FLOAT64
        ) AS preco_original,
        SAFE_CAST(REPLACE(REPLACE(REPLACE(preco_pix, 'R$', ''), '.', ''), ',', '.') AS FLOAT64) as preco_pix,
        SAFE_CAST(num_parcelas AS INT64) AS num_parcelas,
        SAFE_CAST(REPLACE(REPLACE(REPLACE(valor_parcela, 'R$', ''), '.', ''), ',', '.') AS FLOAT64) as valor_parcela,
        SAFE_CAST(REPLACE(REPLACE(REPLACE(valor_total_parcelado, 'R$', ''), '.', ''), ',', '.') AS FLOAT64) as valor_total_parcelado,
        
        -- nota e n de avaliações
        SAFE_CAST(REPLACE(produto_nota, ',', '.') AS FLOAT64) AS produto_nota,
        SAFE_CAST(produto_num_avaliacoes AS INT64) AS produto_num_avaliacoes,
        
        -- infos do vendedor
        eh_full,
        SAFE_CAST(REPLACE(vendedor_reputacao, ',', '.') AS FLOAT64) AS vendedor_reputacao,
        vendedor_qtd_vendas,
        vendedor_entrega_prazo,
        vendedor_atendimento,
        
        -- coluna pra pegar apenas última atualização do dia por produto e vendedor
        ROW_NUMBER() OVER(
            PARTITION BY REGEXP_EXTRACT(url_produto, r'/p/([^/]+)/'), 
                         REGEXP_EXTRACT(url_produto, r'seller_id=([^&]+)'), 
                         DATE(DATETIME(timestamp_coleta, "America/Sao_Paulo")) 
            ORDER BY DATETIME(timestamp_coleta, "America/Sao_Paulo") DESC
        ) as row_num
    FROM source
)

SELECT
    produto_id,
    vendedor_id,
    data_coleta,
    preco_original,
    preco_pix,
    num_parcelas,
    valor_parcela,
    valor_total_parcelado,
    produto_nota,
    produto_num_avaliacoes,
    eh_full,
    vendedor_reputacao,
    vendedor_qtd_vendas,
    vendedor_entrega_prazo,
    vendedor_atendimento,
    timestamp_coleta
FROM deduplicado_diario
WHERE row_num = 1