
    with 
        source as (
            select 
                /* Chave Primária */
                id as enrolusuario_pk

                /* Chave Estrangeira */
                , userid as id_moodle_br
                , enrolid as enrol_fk

                /* Dados de Matricula */
                , timecreated as data_matricula


 

            from {{ source('ambiente_br','mdl_user_enrolments') }}
    )
    select * 
    from source



