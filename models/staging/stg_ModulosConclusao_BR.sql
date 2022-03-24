
    with 
        source as (
            select 
                /* Chave Primária */
                id as modulosconclusao_pk

                /* Chave Estrangeira */
                , coursemoduleid as modulo_fk
                , userid as id_moodle_br

                /* Dados de conclusao do curso */
                ,            
                  case 
                    when completionstate = 0 then 'Não'
                    else 'Sim'
                  end as Concluido  
                , timemodified as Data_Conclusao

            from {{ source('ambiente_br','mdl_course_modules_completion') }}
    )
    select * 
    from source



