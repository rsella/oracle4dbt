
/*
    Assert that the dbt_valid_from of the latest record
    is equal to the dbt_valid_to of the previous record
*/


select
    snap1.id,
    snap1.dbt_valid_from as new_valid_from,
    snap2.dbt_valid_from as old_valid_from,
    snap2.dbt_valid_to as old_valid_to

from (select * from {{ ref('my_slow_snapshot') }}) snap1
join (select * from {{ ref('my_slow_snapshot') }}) snap2 on snap1.id = snap2.id
where snap1.dbt_valid_to is null
  and snap2.dbt_valid_to is not null
  and snap1.dbt_valid_from != snap2.dbt_valid_to
