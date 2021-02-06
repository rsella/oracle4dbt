# oracle4dbt
Oracle adapter for DBT (Data Build Tool)

## :warning: PLEASE READ THIS :warning:
This adapter is not suitable for production usage. It's just a way to test DBT and its opinionated workflow if you currently use an Oracle database.

The intended usage is to point at a **TEST** database and try out the different features.

## Installation
The adapter uses the cx_Oracle package, so you need to have an Oracle client installed on your system.
You can either use the client that comes in with your DB or download the instant client from Oracle.

https://www.oracle.com/database/technologies/instant-client.html


## Supported versions

DBT: 
* tested with 0.19.0
* should probably work with >= 0.19.0

Oracle:
* tested with DB version 18c
* should probably work with >= 12.2 (identifiers length >= 128 chars)

## Profile configuration
Add the following into your `profiles.yml`
```yaml
default:
  outputs:

    dev:
      type: oracle
      threads: 4
      host: localhost
      port: 1521
      service: XEPDB1
      username: SYS
      password: root
      as_sysdba: true
      nls_date_format: 'YYYY-MM-DD HH24:MI:SS'
      schema: dbt_test
```

To generate a sample profile.yml you can use

`dbt init [project_name] --adapter oracle`

### Notes:
* **host**, **port** and **service** are used while connecting to the database `host:port/service_name`


* **as_sysdba** is optional, defaults to false
  

* **nls_date_format** is optional, defaults to none.

    if you provide a value, it will be set on every session opened by DBT.
  
    Also:
    * nls_timestamp_format will be set as `{nls_date_format}XFF`
    * nls_timestamp_tz_format will be set as `{nls_date_format}XFF TZR`
    

## Features
Apart from what is listed in the Caveats sections, every DBT functionality is expected to be working as intended.

This includes:
* materializations
* snapshots
* tests
* custom schemas
* hooks
* seeds

If something is  off please open an issue

## Caveats
DBT is thought from the ground up to be run against a set of databases (postgres, redshift, snowflake, ...); therefore in some cases Oracle behave differently.

The main caveats are listed below. 
Please note that if you write SQL that works on Oracle it should be good to go in DBT too.

### CTE / with clauses / ephemeral models
Oracle doesn't support nested with clauses. 

Put simply, you can't do:
```
WITH cte_a AS (
    WITH cte_b AS (
        ...
    )
    SELECT * FROM cte_b
)
SELECT * FROM cte_a
```

This has some implications, the most important one is that you can't `ref` an ephemeral model from another ephemeral model

The error message is usually pretty clear: just refactor your code

### CTE names
In some cases, if you name the CTE as the base table an error is raised
```
WITH table_a AS (
    SELECT * FROM schema_a.table_a
)
```

### NLS
In some cases DBT performs conversions between date/timestamps and python `str`.
For this reason explicitly setting a NLS_DATE_FORMAT in the profile may avoid problems.

If you keep getting errors like `ORA-01843: not a valid month` please consider setting the profile parameter.
It will be set on every session opened by DBT



## Tests
Testing is done using Tox.\
There are four kind of tests available:
* unit tests
* integration tests
* dbt adapter tests
* sample projects

### Unit tests
Forked from DBT main repo and adapted to Oracle (mainly SQL changes)

Launch with `tox -e unit`

### Integration tests
Forked from DBT main repo, adapted to Oracle (mainly SQL changes). 

Postgres tests are used, as these are the one more closely related to Oracle. Tests of specific Postgres commands are disabled (eg. vacuum commands)

Launch with `tox -e integration`


### Dbt adapter tests
Forked from https://github.com/fishtown-analytics/dbt-adapter-tests and adapted to Oracle.

Launch with `tox -e dbt-adapter`


### Sample projects
Jaffle Shop and Attribute Playbook projects are available to launch. Some models have been changed to make them compatible with Oracle syntax.

Launch with:
* jaffle_shop: `tox -e jaffle-shop`
* attribution_playbook: `tox -e attribution-playbook`



## Contributing
Every contribution is welcome and encouraged.

Please note that this is a side project and replies may need some time