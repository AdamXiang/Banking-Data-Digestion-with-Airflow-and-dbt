{{ config(materialized='view') }}

with ranked as (
    select
        value:id::string            as customer_id,
        value:first_name::string    as first_name,
        value:last_name::string     as last_name,
        value:email::string         as email,
        value:created_at::timestamp as created_at,
        current_timestamp       as load_timestamp,
        row_number() over (
            partition by value:id::string
            order by value:created_at desc
        ) as rn
    from {{ source('raw', 'customers') }}
)

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    load_timestamp
from ranked
where rn = 1