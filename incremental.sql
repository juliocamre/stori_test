{{
    config(
        materialized = 'incremental',
        unique_key = 'order_month',
        incremental_strategy = 'merge',
        alias = 'incremental_orders',
        schema = 'marts'
    )
}}


with new_sales_data as (
    select
        date_trunc('month', order_date) as order_month,
        count(order_id) as new_orders_count,
        sum(amount) as new_sales_amount
    from {{ source('raw', 'orders') }}
    
    {% if is_incremental() %}
    where order_date >= (select max(order_month) from {{ this }})
    {% endif %}
    
    group by 1, 2, 3
)

select
    order_month,
    year,
    month,
    new_orders_count as number_of_orders,
    new_sales_amount as total_sales_amount,
    new_sales_amount / nullif(new_orders_count, 0) as average_order_value,
    current_timestamp as load_timestamp
from new_sales_data
