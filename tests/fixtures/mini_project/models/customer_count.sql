select count(*) as total_customers
from {{ source('waffle_hut', 'customers') }}
