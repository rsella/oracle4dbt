{% set col_a = '"col_A"' %}
{% set col_b = '"col_B"' %}
{% if adapter.type() == 'bigquery' %}
    {% set col_a = '`col_A`' %}
    {% set col_b = '`col_B`' %}
{% elif adapter.type() == 'oracle' %}
	{% set col_a = '"COL_A"' %}
	{% set col_b = '"COL_B"' %}
{% endif %}

{{config(
    materialized = 'incremental',
    unique_key = col_a,
    incremental_strategy = var('strategy')
    )}}

select
{{ col_a }}, {{ col_b }}
from {{ref('seed')}}
