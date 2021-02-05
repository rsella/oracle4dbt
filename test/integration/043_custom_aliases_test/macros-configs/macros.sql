
{#-- Verify that the config['alias'] key is present #}
{% macro generate_alias_name(custom_alias_name, node) -%}
    {%- if custom_alias_name is none -%}
        {{ node.name }}
    {%- else -%}
        custom_{{ node.config['alias'] | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro string_literal(s) -%}
  {{ adapter.dispatch('string_literal', packages=['test'])(s) }}
{%- endmacro %}

{% macro default__string_literal(s) %}
    '{{ s }}'::text
{% endmacro %}

{% macro oracle__string_literal(s) %}
    CAST('{{ s }}' AS VARCHAR2(4000 CHAR))
{% endmacro %}

{% macro bigquery__string_literal(s) %}
    cast('{{ s }}' as string)
{% endmacro %}
