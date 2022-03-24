
    with 
        source as (
            select 
                /* Chave Primária */
                id as enrol_pk

                /* Chave Estrangeira */
                , courseid as formacao_fk


 

            from {{ source('ambiente_br','mdl_enrol') }}
    )
    select * 
    from source



