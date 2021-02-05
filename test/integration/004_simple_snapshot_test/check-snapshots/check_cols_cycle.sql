
{% snapshot check_cols_cycle %}

    {{
        config(
            target_database=database,
            target_schema=schema,
            unique_key='id',
            strategy='check',
            check_cols=['color']
        )
    }}

    {% if var('version') == 1 %}

        select 1 as id, 'red' as color from dual union all
        select 2 as id, 'green' as color from dual

    {% elif var('version') == 2 %}

        select 1 as id, 'blue' as color from dual union all
        select 2 as id, 'green' as color from dual

    {% elif var('version') == 3 %}

        select 1 as id, 'red' as color from dual union all
        select 2 as id, 'pink' as color from dual

    {% else %}
        {% do exceptions.raise_compiler_error("Got bad version: " ~ var('version')) %}
    {% endif %}

{% endsnapshot %}
