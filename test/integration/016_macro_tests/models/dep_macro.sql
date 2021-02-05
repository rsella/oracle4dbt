
-- not supported syntax in remote dbt_integration_project
-- dbt_integration_project.do_something("arg1", "arg2")

select
        CAST('arg1' as varchar2(4000 char)) as foo,
        CAST('arg2' as varchar2(4000 char)) as bar
        from dual