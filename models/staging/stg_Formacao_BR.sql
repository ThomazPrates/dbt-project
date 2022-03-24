
    with 
        source as (
            select 
                /* Chave Primária */
                id as formacao_pk

                /* Chave Estrangeira */
                , category as categoria_fk

                /* Dados da Formacao */
                , fullname as nome_formacao
                , shortname as sigla_formacao
                , timecreated as data_criacao
 

            from {{ source('ambiente_br','mdl_course') }}
    )
    select * 
    from source



