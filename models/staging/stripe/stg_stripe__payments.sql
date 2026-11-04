with base as (
    select * from {{ source('stripe', 'payments') }}
),

transform as (
    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status as payment_status,
        cast({{ cents_to_dollars('amount') }} as double) as amount
    from base
)

select * from transform