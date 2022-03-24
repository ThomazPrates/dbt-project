
    with 
        selected as (
            select 
                curso_pk
                , formacao_fk
                , nome_curso
                , 'Ambiente_BR'  as ambiente
 

            from {{ ref('stg_Curso_BR') }} 
        )
        , transformed as (
            select
                row_number() over (order by curso_pk) as curso_sk
                , *
            from selected
        )


    select * 
    from transformed



