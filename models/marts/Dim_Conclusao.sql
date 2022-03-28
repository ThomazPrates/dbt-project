
    with 
        cursos as (
            select 
                curso_sk
                , curso_fk
                , formacao_fk
                , nome_curso
                , total_atividades
            from {{ ref('Dim_Curso') }} 
        )

        , modulo as (
            select
                modulo_pk
                , curso_fk
                , modulos
            from {{ref('stg_Modulos_BR')}}            
        )
        , moduloconclusao as (
            select
                modulosconclusao_pk
                , modulo_fk
                , id_moodle_br
                , concluido
                , modulos
                , curso_fk 
            from {{ref('stg_ModulosConclusao_BR')}} as moduloconclusao
            left join modulo on moduloconclusao.modulo_fk = modulo.modulo_pk where modulos in ('17','19','7')
        )       
        , add_modulos as (
            select
                modulosconclusao_pk
                , modulo_fk
                , id_moodle_br
                , concluido
                , modulos
                , cursos.curso_fk
                , cursos.nome_curso
                , cursos.total_atividades
            from moduloconclusao 
            left join cursos on moduloconclusao.curso_fk = cursos.curso_fk       
        )
        , contador_atividades as (
            SELECT COUNT(modulosconclusao_pk) as atividades_concluidas, modulosconclusao_pk, id_moodle_br, curso_fk
            FROM add_modulos where concluido = 'Sim'
            GROUP BY  id_moodle_br , curso_fk, modulosconclusao_pk
        )
        , final as (
            select 
                contador_atividades.modulosconclusao_pk as modulosconclusao_fk
                , contador_atividades.id_moodle_br
                , cursos.formacao_fk
                , cursos.curso_fk
                , cursos.nome_curso
                , contador_atividades.atividades_concluidas
                , cursos.total_atividades
                , concat(ROUND( ( (1.0*atividades_concluidas) / total_atividades)* 100, 2), '%' ) as porcentagem_conclusao
                , 'Ambiente_BR'  as ambiente
            from contador_atividades 
            left join cursos on  contador_atividades.curso_fk = cursos.curso_fk
        )
        , transformed as (
            select
                row_number() over (order by modulosconclusao_fk) as conclusao_sk
                , *
            from final           
        )
    select *
    from transformed
