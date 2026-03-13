{{ config(materialized='view') }}

SELECT
    value:id::string                 AS transaction_id,
    value:account_id::string         AS account_id,
    value:amount::float              AS amount,
    value:txn_type::string           AS transaction_type,
    value:related_account_id::string AS related_account_id,
    value:status::string             AS status,
    value:created_at::timestamp      AS transaction_time,
    CURRENT_TIMESTAMP            AS load_timestamp
FROM {{ source('raw', 'transactions') }}