{{
    config(
        materialized='table',
        alias='orders_per_month',
        schema='staging'
    )
}}

SELECT 
    EXTRACT(YEAR FROM order_date) as year,
    EXTRACT(MONTH FROM order_date) as month,
    COUNT(order_id) as order_count
FROM {{ source('raw', 'orders') }}
GROUP BY 
    EXTRACT(YEAR FROM order_date),
    EXTRACT(MONTH FROM order_date)
ORDER BY order_month DESC
