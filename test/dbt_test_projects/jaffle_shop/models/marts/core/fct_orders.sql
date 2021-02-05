{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (

    select * from {{ ref('stg_orders') }}

),

stg_order_payments as (

    select * from {{ ref('order_payments') }}

),

final as (

    select
        order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,

        {% for payment_method in payment_methods -%}

        stg_order_payments.{{payment_method}}_amount,

        {% endfor -%}

        stg_order_payments.total_amount as amount

    from orders

    left join stg_order_payments  using (order_id)

)

select * from final
