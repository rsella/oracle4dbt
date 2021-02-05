
{{
    config(
        materialized='view'
    )
}}


with t as (

    select * from {{ ref('view_model') }}

)

select TRUNC(updated_at, 'YEAR') as year,
       count(*) AS count
from t
group by TRUNC(updated_at, 'YEAR')
