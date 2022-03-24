
    with 
        source as (
            select 
                /* Chave Primária */
                id as curso_pk

                /* Chave Estrangeira */
                , course as formacao_fk


                /* Dados do curso */
                , name as nome_curso

 

            from {{ source('ambiente_br','mdl_course_sections') }}
    )
    select * 
    from source



