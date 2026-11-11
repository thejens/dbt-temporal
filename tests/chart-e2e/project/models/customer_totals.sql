select customer_id, sum(amount) as total
from {{ ref('orders') }}
group by customer_id
