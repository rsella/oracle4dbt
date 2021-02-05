
{{
    config(
        materialized = 'view',
        tags = 'bi'
    )
}}

with all_users as (

    select * from {{ ref('users') }}

)

select
    gender,
    count(*) as ct
from all_users
group by gender
