with customers as (

    select * from {{ ref('stg_customers') }}

),

all_customer_orders as (

    select * from {{ ref('customer_orders') }}

),

all_customer_payments as (

    select * from {{ ref('customer_payments') }}

),

final as (

    select
        customer_id,
        all_customer_orders.first_order,
        all_customer_orders.most_recent_order,
        all_customer_orders.number_of_orders,
        all_customer_payments.total_amount as customer_lifetime_value

    from customers

    left join all_customer_orders using (customer_id)

    left join all_customer_payments using (customer_id)

)

select * from final
