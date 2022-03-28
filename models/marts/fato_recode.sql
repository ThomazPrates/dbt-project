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
    , conclusao as (
        select *
        from {{ref('Dim_Conclusao')}}
    )
    , modulo_com_sk as (
        select
            modulo.modulo_sk
            , modulo.modulosconclusao_pk
            , usuario.usuario_sk as usuario_fk
            , formacao.formacao_sk as formacao_fk
            , curso.curso_sk as curso_fk
            , modulo.concluido
            , modulo.Data_Conclusao
            , conclusao.atividades_concluidas
            , conclusao.total_atividades
            , conclusao.porcentagem_conclusao
            , modulo.ambiente
        from {{ref('Dim_Modulo')}} as modulo
        left join usuario on modulo.id_moodle_br = usuario.id_moodle_br
        left join formacao on modulo.formacao_fk = formacao.formacao_pk
        left join curso on modulo.curso_fk = curso.curso_fk
        left join conclusao on modulo.modulosconclusao_pk = conclusao.modulosconclusao_fk
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
            , enrol_com_sk.formacao_fk
            , modulo_com_sk.curso_fk
            , modulo_com_sk.concluido
            , modulo_com_sk.Data_Conclusao
            , modulo_com_sk.atividades_concluidas
            , modulo_com_sk.total_atividades
            , modulo_com_sk.porcentagem_conclusao
            , modulo_com_sk.ambiente
            , enrol_com_sk.data_matricula
        from modulo_com_sk
        left join enrol_com_sk on modulo_com_sk.usuario_fk = enrol_com_sk.usuario_fk
-- fazer join da tabela usuario . 

    )
    , transformed_final as (
        select
            row_number() over (order by modulo_sk) as fato_pk ,
            *
        from final
    )

select * from transformed_final 