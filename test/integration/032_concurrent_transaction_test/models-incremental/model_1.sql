
{{ config(materialized='incremental', unique_key='id') }}

-- incremental model
select 1 as id from dual

{% if is_incremental() %}
    where TRUE
{% endif %}
