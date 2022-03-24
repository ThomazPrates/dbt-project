
    with 
        source as (
            select 
                /* Chave Primária */
                id as endereco_pk

                /* Dados do Endereço */
                , 
                  case
                    when estado LIKE 'sao%' then 'SP'
                    when estado LIKE 'são%' then 'SP'
                    when estado = 'sp' then 'SP'
                    when estado LIKE '%paulo' then 'SP'
                    when estado LIKE '%Paulo' then 'SP'
                    else estado
                  end as estado
                , cidade 

            from {{ source('ambiente_br','endereco') }}
    )
    select * 
    from source
