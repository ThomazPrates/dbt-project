
    with 
        usuarios as (
            select 
                 usuario_sgabr_pk
                , id_moodle_br
                , endereco_fk
                , ced_fk 
                , nomecompleto
                , email
                , cpf
                , perfil_usuario
                , datanascimento
                , data_cadastro
                , 'Ambiente_BR'  as ambiente
 
            from {{ ref('stg_Usuarios_BR') }} 
        )

        , enderecos as (
            select 
                endereco_pk
                , estado
                , cidade 

            from  {{ ref('stg_Enderecos_BR') }}
        )

        , add_enderecos as (
            select * 
            from usuarios
            left join enderecos on usuarios.endereco_fk = enderecos.endereco_pk
        )
            , transformed as (
            select
                row_number() over (order by usuario_sgabr_pk) as usuario_sk ,
                 *
            from add_enderecos
        )
    select 
        usuario_sk
        , usuario_sgabr_pk
        , id_moodle_br
        , ced_fk
        , nomecompleto
        , email
        , cpf
        , perfil_usuario
        , estado
        , cidade
        , datanascimento
        , data_cadastro
        , ambiente
    from transformed
