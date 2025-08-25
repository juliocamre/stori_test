{{
    config(
        severity = 'error',
        tags = ['data_quality', 'montos']
    )
}}

select
    order_id,
    customer_id,
    order_date,
    amount,
    'Invalid Amount' as mistake
from {{ source('raw', 'orders') }}
where amount < 0
