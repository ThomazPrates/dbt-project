
    with 
        source as (
            select 
                /* Chave Primária */
                id as modulo_pk

                /* Chave Estrangeira */
                , course as formacao_fk
                , section as curso_fk


 

            from {{ source('ambiente_br','mdl_course_modules') }}
    )
    select * 
    from source



