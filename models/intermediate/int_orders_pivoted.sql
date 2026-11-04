{# {% set order_statuses = ['completed','placed','return_pending','returned','shipped'] %} #}

-- Dynamically get distinct order statuses
{% set distinct_status_query %}
    select distinct order_status from {{ ref('stg_jaffle_shop__orders') }} order by 1
{% endset %}

{% set results = run_query(distinct_status_query) %}

{% if execute %}
    {% set order_statuses = results.columns[0].values() %}
{% else %}
    {% set order_statuses = [] %}
{% endif %}

with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select order_id, sum(amount) as amount 
    from {{ ref('stg_stripe__payments') }}
    where payment_status = 'success'
    group by 1
),

pivoted as (
    select
        order_date,
        {% for status in order_statuses -%}
            SUM(CASE WHEN order_status = '{{ status }}' THEN amount ELSE 0 END) as {{ status }}
            {%- if not loop.last -%} , {% endif %}
        {% endfor %}
    from orders as o
        left join payments as p on o.order_id = p.order_id
    group by 1 order by 1 desc
)

select *
from pivoted
where 1=1
    and {{ date_window('order_date') }}

