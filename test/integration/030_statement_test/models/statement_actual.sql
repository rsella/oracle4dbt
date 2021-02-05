
-- {{ ref('seed') }}

{%- call statement('test_statement', fetch_result=True) -%}

  select
    count(*) as "num_records"

  from {{ ref('seed') }}

{%- endcall -%}

{% set result = load_result('test_statement') %}

{% set res_table = result['table'] %}
{% set res_matrix = result['data'] %}

{% set matrix_value = res_matrix[0][0] %}
{% set table_value = res_table[0]['num_records'] %}

select CAST('matrix' AS VARCHAR2(26 CHAR)) as source, {{ matrix_value }} as value from dual
union all
select CAST('table' AS VARCHAR2(26 CHAR)) as source, {{ table_value }} as value from dual
