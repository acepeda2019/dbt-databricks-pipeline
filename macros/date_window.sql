{%- macro date_window(
    column_name,
    lookback_days=none,
    default_date='2020-01-01'
) %}
    {%- set lookback_days = lookback_days if lookback_days is not none else var('lookback_days', 3500) %}
    {%- set dev_anchor = var('date_window_anchor', 'current_date()') %}

    {%- if target.name == 'dev' %}
        {{ column_name }} >= date_sub({{ dev_anchor }}, {{ lookback_days }})
    {%- elif is_incremental() %}
        {{ column_name }} >= (
            select coalesce( date_sub( max({{ column_name }}), {{lookback_days}}), cast('{{default_date}}' as date) )
            from {{ this }}
        )
    {%- else %}
        1=1
    {%- endif %}
    {#- non-dev + not incremental: neither branch fires, no filter, full history builds -#}

{%- endmacro -%}
