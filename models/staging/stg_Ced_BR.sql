
    with 
        source as (
            select 
                /* Chave Primária */
                id as ced_pk

                /* Dados da Instituição */
                , nomeinstituicao
                , cnpj 
                , createat as Data_Cadastro
 

            from {{ source('ambiente_br','ced') }}
    )
    select * 
    from source



