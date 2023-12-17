{{ 
    config(
        materialized='incremental'
    ) 
}}

{% if is_incremental() %}

select
    id + 1 as id,
    end_date as start_date,
    end_date + interval '1' month as end_date,
    now() as created_at
from {{ this }}
    where id = (select max(id) from {{ this }})

{% else %}

select
    1 as id,
    '2016-09-01'::date as start_date,
    '2016-10-01'::date as end_date,
    now() as created_at

{% endif %}