
{{ config(materialized=var('materialized')) }}

select '{{ var("materialized") }}' as materialization from dual

{% if var('materialized') == 'incremental' and is_incremental() %}
    where 'abc' != (select max(materialization) from {{ this }})
{% endif %}
