{% snapshot snapshot_actual %}
    {# this custom strategy doesn't exist  in the 'dbt' package #}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='dbt.custom',
            updated_at='updated_at',
        )
    }}
    select * from {{target.schema}}.seed

{% endsnapshot %}
