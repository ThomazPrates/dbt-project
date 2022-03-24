
    with 
        enrolusuario as (
            select 
                enrolusuario_pk
                , id_moodle_br
                , enrol_fk 
                , data_matricula
                , 'Ambiente_BR'  as ambiente
 

            from {{ ref('stg_EnrolUsuario_BR') }} 
        )
        , enrol as (
            select 
                enrol_pk
                , formacao_fk
 
            from {{ ref('stg_Enrol_BR') }} 
        )
        , add_enrol as (
            select * 
            from enrolusuario
            left join enrol on enrolusuario.enrol_fk = enrol.enrol_pk
        )
        , transformed as (
            select
                row_number() over (order by enrolusuario_pk) as enrol_sk
                , *
            from add_enrol
        )
    select  
        enrol_sk
        , enrolusuario_pk
        , id_moodle_br
        , formacao_fk
        , data_matricula
        , ambiente
    from transformed



