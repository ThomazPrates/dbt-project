
    with 
        source as (
            select 
                /* Chave Primária */
                id

                /* Chave Estrangeira*/
                , questionarioid

                /* respostas do questionario */
                , valorresposta
                
            from {{ source('ambiente_br','resposta') }}
    )
    select * 
    from source



