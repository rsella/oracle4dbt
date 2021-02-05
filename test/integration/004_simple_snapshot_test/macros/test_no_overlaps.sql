{% macro get_snapshot_unique_id() -%}
    {{ return(adapter.dispatch('get_snapshot_unique_id')()) }}
{%- endmacro %}

{% macro default__get_snapshot_unique_id() -%}
  {% do return("id || '-' || first_name") %}
{%- endmacro %}


{% macro bigquery__get_snapshot_unique_id() -%}
    {%- do return('concat(cast(id as string), "-", first_name)') -%}
{%- endmacro %}

{#
    mostly copy+pasted from dbt_utils, but I removed some parameters and added
    a query that calls get_snapshot_unique_id
#}
{% macro test_mutually_exclusive_ranges(model) %}

with base as (
    select {{ get_snapshot_unique_id() }} as dbt_unique_id,
    src.*
    from {{ model }} src
),
window_functions as (

    select
        dbt_valid_from as lower_bound,
        coalesce(dbt_valid_to, to_date('2099-01-01 00:00:01', 'YYYY-MM-DD HH24:MI:SS')) as upper_bound,

        lead(dbt_valid_from) over (
            partition by dbt_unique_id
            order by dbt_valid_from
        ) as next_lower_bound,

        CASE WHEN row_number() over (
            partition by dbt_unique_id
            order by dbt_valid_from desc
        ) = 1 THEN 1 ELSE 0 END as is_last_record

    from base

),

calc as (
    -- We want to return records where one of our assumptions fails, so we'll use
    -- the `not` function with `and` statements so we can write our assumptions nore cleanly
    select
        src.*,

        -- For each record: lower_bound should be < upper_bound.
        -- Coalesce it to return an error on the null case (implicit assumption
        -- these columns are not_null)
        -- coalesce(
        --     lower_bound < upper_bound,
        --     is_last_record
        -- ) as lower_bound_less_than_upper_bound,

        -- For each record: upper_bound {{ allow_gaps_operator }} the next lower_bound.
        -- Coalesce it to handle null cases for the last record.
        -- coalesce(
        --     upper_bound = next_lower_bound,
        --     is_last_record,
        --     false
        -- ) as upper_bound_equal_to_next_lower_bound

        -- ORACLE FIX
        CASE
            WHEN lower_bound IS NULL OR upper_bound IS NULL THEN is_last_record
            ELSE
                CASE
                    WHEN lower_bound < upper_bound THEN 1
                    ELSE 0
                END
        END AS lower_bound_less_than_upper_bound,

        CASE
            WHEN next_lower_bound IS NULL OR upper_bound IS NULL THEN NVL(is_last_record, 0)
            ELSE
                CASE
                    WHEN next_lower_bound = upper_bound THEN 1
                    ELSE 0
                END
        END AS upper_bound_equal_to_next_lower_bound

    from window_functions src

),

validation_errors as (

    select
        *
    from calc

    where not(
        -- THE FOLLOWING SHOULD BE TRUE --
        lower_bound_less_than_upper_bound = 1
        and upper_bound_equal_to_next_lower_bound = 1
    )
)

select count(*) from validation_errors
{% endmacro %}
