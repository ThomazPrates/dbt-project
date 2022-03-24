
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
        , concluido
        , Data_Conclusao
        , ambiente
    from transformed

/*
LEFT JOIN (
	SELECT
		`rec_lms-new`.mdl_course_sections.id corse_sec_id,
		`rec_lms-new`.mdl_course_sections.name Curso,
		`rec_lms-new`.mdl_course_sections.course id_curso,
		count(`rec_lms-new`.mdl_course_modules.id) Quantidade_total
		
	FROM
		`rec_lms-new`.mdl_course_modules 
		JOIN `rec_lms-new`.mdl_course_sections ON `rec_lms-new`.mdl_course_sections.id = `rec_lms-new`.mdl_course_modules.section
	WHERE
		mdl_course_modules.course = @cursoID
		AND `rec_lms-new`.mdl_course_modules.module IN (17, 19, 7)
		AND `rec_lms-new`.mdl_course_modules.completion > 0
	GROUP BY Curso, id_curso, corse_sec_id
	
-- ) AS C ON C.Curso = B.Curso
) AS C ON C.id_curso = A.id_curso

*/