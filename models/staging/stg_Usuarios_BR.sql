
    with 
        source as (
            select 
                /* Chave Primária */
                id as usuario_sgabr_pk

                /* Chave Estrangeira */
                , idmoodle  as id_moodle_br
                , enderecoid as endereco_fk
                , cedid as ced_fk

                /* Dados do Usuário */
                , nomecompleto
                , email
                , cpf

                , 
                  case 
                    when perfil = 0 then 'Aluno'
                    when perfil = 1 then 'Educador'
                    when perfil = 2 then 'Responsavel_Ced'
                    when perfil = 3 then 'Administrador'
                    else 'Nao_Informado'
                  end as Perfil_Usuario
                , datanascimento
                , createat as Data_Cadastro
 

            from {{ source('ambiente_br','usuario') }}
    )
    select * 
    from source



