with    
    usuario as (
        select *
        from {{ref('Dim_Usuario')}}
    )
    , questionario as (
        select *
        from {{ref('Dim_Questionario')}}
    )
    , ced as (
        select *
        from {{ref('Dim_Ced')}}
    )
    , formacao as (
        select *
        from {{ref('Dim_Formacao')}}
    )
    , curso as (
        select *
        from {{ref('Dim_Curso')}}
    )
    , modulo_com_sk as (
        select
            modulo.modulo_sk
            , usuario.usuario_sk as usuario_fk
            , formacao.formacao_sk as formacao_fk
            , curso.curso_sk as curso_fk
            , modulo.concluido
            , modulo.Data_Conclusao
            , modulo.ambiente
        from {{ref('Dim_Modulo')}} as modulo
        left join usuario on modulo.id_moodle_br = usuario.id_moodle_br
        left join formacao on modulo.formacao_fk = formacao.formacao_pk
        left join curso on modulo.curso_fk = curso.curso_pk


    )
    , enrol_com_sk as (
        select
            enrol_sk
            , enrol.enrolusuario_pk
            , usuario.usuario_sk as usuario_fk
            , formacao.formacao_sk as formacao_fk
            , enrol.data_matricula
            , enrol.ambiente
        from {{ref('Dim_Enrol')}} as enrol
        left join usuario on enrol.id_moodle_br = usuario.id_moodle_br
        left join formacao on enrol.formacao_fk = formacao.formacao_pk
    )
    , final as (
        select 
            modulo_com_sk.modulo_sk
            , modulo_com_sk.usuario_fk
            , modulo_com_sk.curso_fk
            , modulo_com_sk.concluido
            , modulo_com_sk.Data_Conclusao
            , modulo_com_sk.ambiente
            , enrol_com_sk.data_matricula
        from modulo_com_sk
        left join enrol_com_sk on modulo_com_sk.usuario_fk = enrol_com_sk.usuario_fk
    )

select * from final 