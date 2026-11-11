{{ config(materialized='table') }}
select n as order_id, n % 3 as customer_id, n * 10 as amount
from generate_series(1, 30) as n
