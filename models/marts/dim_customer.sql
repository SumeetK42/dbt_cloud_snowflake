with customers as (
select * from {{ ref ('stg_jaffle_shop__customers') }}
),
orders as (
   select * from {{ ref('fct_orders') }}
),
customer_orders as (
    select  
      customer_id,
      max(order_date) as most_recent_order_date,
      min(order_date) as first_order_date,
      count(*) as total_orders,
      sum(amount) as lifetime_value
    from orders
    group by 1
),
final as (
    select 
     a.customer_id,
     a.first_name,
     a.last_name,
     b.first_order_date,
     b.most_recent_order_date,
     coalesce(total_orders,0) as no_of_orders,
     b.lifetime_value
    from customers  a 
    left join customer_orders b  on a.customer_id = b.customer_id
)
select * from final