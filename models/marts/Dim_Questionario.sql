
    with 
        questionario as (
            select 
                questionariousuario_pk
                , resposta
                , questionario_fk
                , usuario_sgabr_fk
                , 'Ambiente_BR'  as ambiente
 

            from {{ ref('stg_QuestionarioUsuario_BR') }} 
        )
        , questionario_valores as (
            select 
                questionario_pk 
                , valorquestionario
                , perfil_usuario 
 

            from {{ ref('stg_Questionario_BR') }} 
        )
        , add_valores as (
            select * 
            from questionario
            left join questionario_valores on questionario.questionario_fk = questionario_valores.questionario_pk
        )
        , transformed as (
            select
                row_number() over (order by questionariousuario_pk) as questionario_sk
                , *
            from add_valores
        )


    select  
        questionario_sk
        , usuario_sgabr_fk
        , valorquestionario as pergunta
        , resposta
        , perfil_usuario
        , ambiente
    from transformed



