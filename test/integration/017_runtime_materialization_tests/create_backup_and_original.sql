
create view {schema}.materialized as (
    select 1 as id from dual
);

create table {schema}.materialized__dbt_backup (
	id NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,
);
