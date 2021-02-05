
{{
    config(
        materialized='table'
    )
}}

select

    -- compile-time variables
    '{{ this }}'        as "this",
    '{{ this.name }}'   as "this.name",
    '{{ this.schema }}' as "this.schema",
    '{{ this.table }}'  as "this.table",

    '{{ target.service }}'  as "target.service",
    '{{ target.host }}'    as "target.host",
    '{{ target.name }}'    as "target.name",
    '{{ target.schema }}'  as "target.schema",
    '{{ target.type }}'    as "target.type",
    '{{ target.username }}'    as "target.username",
    '{{ target.get("pass", "nopass") }}'    as "target.password", -- not actually included, here to test that it is _not_ present!
    {{ target.port }}      as "target.port",
    {{ target.threads }}   as "target.threads",

    -- runtime variables
    '{{ run_started_at }}' as "run_started_at",
    '{{ invocation_id }}'  as "invocation_id",

    '{{ env_var("DBT_TEST_013_ENV_VAR") }}' as "env_var"
from
    dual
