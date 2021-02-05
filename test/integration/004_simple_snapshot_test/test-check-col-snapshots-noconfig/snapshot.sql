{% snapshot snapshot_actual %}
    select * from {{schema}}.seed
{% endsnapshot %}

{# This should be exactly the same #}
{% snapshot snapshot_checkall %}
	{{ config(check_cols='all') }}
    select * from {{schema}}.seed
{% endsnapshot %}
