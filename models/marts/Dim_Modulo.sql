
    with 
        moduloconclusao as (
            select 
                modulosconclusao_pk
                , modulo_fk
                , id_moodle_br 
                , concluido
                , Data_Conclusao
                , 'Ambiente_BR'  as ambiente
 

            from {{ ref('stg_ModulosConclusao_BR') }} 
        )
        , modulo as (
            select 
                modulo_pk 
                , formacao_fk
                , curso_fk
                , modulos
 
            from {{ ref('stg_Modulos_BR') }} 
        )
        , add_modulos as (
            select * 
            from moduloconclusao
            left join modulo on moduloconclusao.modulo_fk = modulo.modulo_pk
        )
        , transformed as (
            select
                row_number() over (order by modulosconclusao_pk) as modulo_sk
                , *
            from add_modulos
        )
    select  
        modulo_sk
        , modulosconclusao_pk
        , id_moodle_br
        , formacao_fk
        , curso_fk
        , modulos
        , concluido
        , Data_Conclusao
        , ambiente
    from transformed
