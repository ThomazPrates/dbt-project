
    with 
        source as (
            select 
                /* Chave Primária */
                id as questionariousuario_pk

                /* Chave Estrangeira*/
                , usuarioid as usuario_sgabr_fk
                , questionarioid as questionario_fk

                /* respostas do questionario */
                , resposta
                
            from {{ source('ambiente_br','questionariousuario') }}
    )
    select * 
    from source



