{% snapshot snapshot_actual %}
    {# this used to be check_cols=('email',), which ought to be totally valid,
    but isn't because type systems are hard. #}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='concat(cast(id as string) , "-", first_name)',
            strategy='check',
            check_cols=['email'],
        )
    }}
    select * from `{{target.database}}`.`{{schema}}`.seed
{% endsnapshot %}


{# This should be exactly the same #}
{% snapshot snapshot_checkall %}
    {{
        config(
            target_database=var('target_database', database),
            target_schema=schema,
            unique_key='concat(cast(id as string) , "-", first_name)',
            strategy='check',
            check_cols='all',
        )
    }}
    select * from `{{target.database}}`.`{{schema}}`.seed
{% endsnapshot %}
