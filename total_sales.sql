{{
    config(
        materialized='table',
        alias='total_sales',
        schema='staging'
    )
}}

SELECT 
    customer_id,
    SUM(amount) as total_sales_amount
    FROM {{ source('raw', 'orders') }}
GROUP BY customer_id
ORDER BY total_sales_amount DESC
