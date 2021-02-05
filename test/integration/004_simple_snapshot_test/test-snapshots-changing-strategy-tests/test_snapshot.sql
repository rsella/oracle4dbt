
{# /*
    Given the repro case for the snapshot build, we'd
    expect to see both records have color='pink'
    in their most recent rows.
*/ #}

select * from (select 1 as id, 'pink' as color from dual union all select 2 as id, 'pink' as color from dual)
minus
select * from (
    select id, color
    from {{ ref('my_snapshot') }}
    where color = 'pink'
      and dbt_valid_to is null
)

union all

select * from (
    select id, color
    from {{ ref('my_snapshot') }}
    where color = 'pink'
      and dbt_valid_to is null
)
minus
select * from (select 1 as id, 'pink' as color from dual union all select 2 as id, 'pink' as color from dual)
