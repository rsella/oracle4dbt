
{% macro test_column_type(model, column_name, type) %}
    {% set column_name = column_name | upper %}
    {% set cols = adapter.get_columns_in_relation(model) %}

    {% set col_types = {} %}
    {% for col in cols %}
        {% do col_types.update({col.name: col.data_type}) %}
    {% endfor %}

    {% set val = 0 if col_types.get(column_name)|lower == type|lower else 1 %}
    {% if val == 1 and execute %}
        {# I'm so tired of guessing what's wrong, let's just log it #}
        {{ log('Column ' ~ column_name ~ '  has type ' ~ col_types.get(column_name) ~ ', expected ' ~ type, info=True) }}
    {% endif %}

    select {{ val }} as pass_fail from dual

{% endmacro %}
