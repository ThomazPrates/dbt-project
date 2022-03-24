
    with 
        source as (
            select 
                /* Chave Primária */
                id as categoria_pk

                /* Dados da Categoria */
                , name as nome_categoria

 

            from {{ source('ambiente_br','mdl_course_categories') }}
    )
    select * 
    from source



