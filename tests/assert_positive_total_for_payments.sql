select 
order_id,
sum(amount)
from {{ ref('stg_stripe__payments') }} 
group by order_id
having sum(amount) < 0