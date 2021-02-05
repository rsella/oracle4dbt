{% macro no_args() %}
  {% if execute %}
    {% call statement(auto_begin=True) %}
      create table {{ schema }}.no_args (id int);
      commit;
    {% endcall %}
  {% endif %}
{% endmacro %}


{% macro table_name_args(table_name) %}
  {% if execute %}
    {% call statement(auto_begin=True) %}
      create table {{ schema }}.{{ table_name }} (id int);
      commit;
    {% endcall %}
  {% endif %}
{% endmacro %}

{% macro select_something(name) %}
  {% set query %}
    select 'hello, {{ name }}' as name from dual
  {% endset %}
  {% set table = run_query(query) %}
  {% if table.columns[0][0] != 'hello, world' %}
    {% do exceptions.raise_compiler_error("unexpected result: " ~ table) %}
  {% endif %}
{% endmacro %}

{% macro vacuum(table_name) %}
  {% set query %}
    -- vacuum {{ schema }}."{{ table_name }}"
    select 1 from dual
  {% endset %}
  {% do run_query(query) %}
{% endmacro %}


{% macro vacuum_ref(ref_target) %}
  {% set query %}
    -- vacuum {{ ref(ref_target) }}
    select 1 from dual
  {% endset %}
  {% do run_query(query) %}
{% endmacro %}


{% macro log_graph() %}
  {% for node in graph.nodes.values() %}
    {{ log((node | string), info=True)}}
  {% endfor %}
{% endmacro %}
