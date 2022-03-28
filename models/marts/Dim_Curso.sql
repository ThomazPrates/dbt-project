
    with 
        cursos as (
            select 
                curso_pk
                , formacao_fk
                , nome_curso
            from {{ ref('stg_Curso_BR') }} 
        )

        , atividades as (
            select
                modulo_pk
                , curso_fk
                , modulos
            from {{ref('stg_Modulos_BR')}}

        )

        , add_modulos as (
            select *
            from cursos 
            left join atividades on cursos.curso_pk = atividades.curso_fk where modulos in ('17','19','7')               
        )


        , contador_atividades as (
            SELECT COUNT(modulo_pk) as total_atividades, curso_pk
            FROM add_modulos
            GROUP BY  curso_pk
        )
        , final as (
            select 
                cursos_original.curso_pk
                , cursos_original.formacao_fk
                , cursos_original.nome_curso
                , contador_atividades.total_atividades
                , 'Ambiente_BR'  as ambiente
            from {{ ref('stg_Curso_BR') }} as cursos_original
            left join contador_atividades on  cursos_original.curso_pk = contador_atividades.curso_pk
        )
        , transformed as (
            select
                row_number() over (order by curso_pk) as curso_sk
                , *
            from final
            
        )
    select 
        curso_sk
        , curso_pk as curso_fk
        , formacao_fk
        , nome_curso
        , total_atividades
        , ambiente
    from transformed
