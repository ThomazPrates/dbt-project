
    with 
        selected as (
            select 
                ced_pk
                , nomeinstituicao
                , cnpj 
                , Data_Cadastro
                , 'Ambiente_BR'  as ambiente
 

            from {{ ref('stg_Ced_BR') }} 
        )
        , transformed as (
            select
                row_number() over (order by ced_pk) as ced_sk
                , *
            from selected
        )


    select * 
    from transformed



