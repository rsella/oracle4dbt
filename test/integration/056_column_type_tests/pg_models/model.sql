select
    -- 1::smallint as smallint_col,
    -- 2::integer as int_col,
    -- 3::bigint as bigint_col,
    -- 4.0::real as real_col,
    -- 5.0::double precision as double_col,
    -- 6.0::numeric as numeric_col,
    -- '7'::text as text_col,
    -- '8'::varchar(20) as varchar_col
    1 as smallint_col,
    2 as int_col,
    3 as bigint_col,
    4.0 as real_col,
    5.0 as double_col,
    6.0 as numeric_col,
    CAST('7' AS varchar2(1 char)) as text_col,
    CAST('8' as varchar2(20 char)) as varchar_col
from dual