{% macro drop_if_exists(statement_name, drop_query, check_system_view, field_a, value_a, field_b, value_b, field_c, value_c) %}
    {{ adapter.dispatch('drop_if_exists')(statement_name, drop_query, check_system_view, field_a, value_a, field_b, value_b, field_c, value_c) }}
{% endmacro %}

{% macro oracle__drop_if_exists(statement_name, drop_query, check_system_view, field_a, value_a, field_b, value_b, field_c, value_c) -%}
    {% set check_exists_result = check_exists(statement_name, check_system_view, field_a, value_a, field_b, value_b, field_c, value_c) %}
    {% if check_exists_result %}
        {% call statement(statement_name) -%}
            {{ drop_query }}
        {%- endcall %}
    {% endif %}
{% endmacro %}

{% macro check_exists(statement_name, check_system_view, field_a, value_a, field_b, value_b, field_c, value_c) %}
    {{ return(adapter.dispatch('check_exists')(statement_name, check_system_view, field_a, value_a, field_b, value_b, field_c, value_c)) }}
{% endmacro %}

{% macro oracle__check_exists(statement_name, check_system_view, field_a, value_a, field_b, value_b, field_c, value_c) -%}
    {% set result = check_exists_count(statement_name, check_system_view, field_a, value_a, field_b, value_b, field_c, value_c) %}
    {% if result.columns|length > 0 %}
        {{ return(result.columns[0].values()[0] == 1) }}
    {% else %}
        {{ return(false)}}
    {% endif %}
{% endmacro %}

{% macro check_exists_count(statement_name, check_system_view, field_a, value_a, field_b, value_b, field_c, value_c) %}
    {{ return(adapter.dispatch('check_exists_count')(statement_name, check_system_view, field_a, value_a, field_b, value_b, field_c, value_c)) }}
{% endmacro %}

{% macro oracle__check_exists_count(statement_name, check_system_view, field_a, value_a, field_b, value_b, field_c, value_c) -%}
    {% set exists_sql %}
        SELECT
            COUNT(*)
        FROM
            {{ check_system_view }}
        WHERE
            UPPER({{ field_a }}) = UPPER('{{ value_a }}')
            AND UPPER({{ field_b|default('\'1\'') }}) = UPPER('{{ value_b|default(1) }}')
            AND UPPER({{ field_c|default('\'1\'') }}) = UPPER('{{ value_c|default(1) }}')
    {% endset %}
    {{ return(run_query(exists_sql)) }}
{% endmacro %}

