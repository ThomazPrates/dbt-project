
    with 
        formacao as (
            select 
                formacao_pk
                , categoria_fk
                , nome_formacao 
                , sigla_formacao
                , data_criacao
                , 'Ambiente_BR'  as ambiente
 

            from {{ ref('stg_Formacao_BR') }} 
        )
        , categorias as (
            select 
                categoria_pk 
                , nome_categoria 
 
            from {{ ref('stg_Categoria_BR') }} 
        )
        , add_categorias as (
            select * 
            from formacao
            left join categorias on formacao.categoria_fk = categorias.categoria_pk
        )
        , transformed as (
            select
                row_number() over (order by formacao_pk) as formacao_sk
                , *
            from add_categorias
        )
    select  
        formacao_sk
        , formacao_pk
        , nome_categoria
        , nome_formacao
        , sigla_formacao
        , data_criacao
        , ambiente
    from transformed



