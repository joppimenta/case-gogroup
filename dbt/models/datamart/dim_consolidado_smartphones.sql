WITH fato AS (
    SELECT * FROM {{ ref('fct_produtos_diario') }}
),

dim_p AS (
    SELECT 
        produto_id, 
        produto_titulo, 
        url_base, 
        produto_nota AS nota_atual_produto,
        produto_num_avaliacoes AS avaliacoes_atuais_produto
    FROM {{ ref('dim_produtos') }}
),

dim_v AS (
    SELECT 
        id_vendedor, 
        vendedor_nome, 
        vendedor_reputacao AS reputacao_atual_vendedor,
        vendedor_entrega_prazo AS entrega_atual_vendedor
    FROM {{ ref('dim_vendedores') }}
)

SELECT
    f.data_coleta,
    f.timestamp_coleta,
    
    f.produto_id,
    f.vendedor_id,
    p.produto_titulo,
    v.vendedor_nome,

    -- comparativo produtos
    f.produto_nota AS nota_no_dia,
    p.nota_atual_produto,
    f.produto_num_avaliacoes AS avaliacoes_no_dia,
    p.avaliacoes_atuais_produto,

    -- comparativo vendedores
    f.vendedor_reputacao AS reputacao_no_dia,
    v.reputacao_atual_vendedor,
    f.vendedor_entrega_prazo AS entrega_no_dia,
    v.entrega_atual_vendedor,

    f.preco_pix,
    f.preco_original,
    f.num_parcelas,
    f.valor_parcela,
    f.valor_total_parcelado,
    f.eh_full


FROM fato f
LEFT JOIN dim_p p ON f.produto_id = p.produto_id
LEFT JOIN dim_v v ON f.vendedor_id = v.id_vendedor