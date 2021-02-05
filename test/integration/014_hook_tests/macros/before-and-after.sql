
{% macro custom_run_hook(state, target, run_started_at, invocation_id) %}

   insert into {{ target.schema }}.on_run_hook (
        "state",
        "target.dbname",
        "target.host",
        "target.name",
        "target.schema",
        "target.type",
        "target.user",
        "target.pass",
        "target.port",
        "target.threads",
        "run_started_at",
        "invocation_id"
   ) VALUES (
    '{{ state }}',
    '{{ target.service }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.username }}',
    '{{ target.get("password", "") }}',
    {{ target.port }},
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}'
   )

{% endmacro %}
