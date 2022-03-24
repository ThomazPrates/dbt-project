
    with 
        source as (
            select 
                /* Chave Primária */
                id as questionario_pk

                /* perguntas dos questionarios */
                , valorquestionario
                , 
                  case 
                    when perfil = 0 then 'Aluno'
                    when perfil = 1 then 'Educador'
                    when perfil = 2 then 'Responsavel_Ced'
                    when perfil = 3 then 'Administrador'
                    else 'Nao_Informado'
                  end as Perfil_Usuario
 

            from {{ source('ambiente_br','questionario') }}
    )
    select * 
    from source



