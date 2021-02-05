{% snapshot snapshot_actual %}

    {{
        config(
            target_database=var('target_database', database),
            target_schema=var('target_schema', schema),
            unique_key='id || ' ~ "'-'" ~ ' || first_name',
            strategy='test.custom',
            updated_at='updated_at',
        )
    }}
    select * from {{target.schema}}.seed

{% endsnapshot %}
