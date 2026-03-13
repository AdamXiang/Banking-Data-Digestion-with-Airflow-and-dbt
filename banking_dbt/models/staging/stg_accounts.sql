{{ config(materialized='view') }}

with ranked as (
    select
        value:id::string            as account_id,
        value:customer_id::string   as customer_id,
        value:account_type::string  as account_type,
        value:balance::float        as balance,
        value:currency::string      as currency,
        value:created_at::timestamp as created_at,
        current_timestamp       as load_timestamp,
        row_number() over (
            partition by value:id::string
            order by value:created_at desc
        ) as rn
    from {{ source('raw', 'accounts') }}
)

select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    load_timestamp
from ranked
where rn = 1