
select
    '{{ var("variable_1") }}' as var_1,
    '{{ var("variable_2")[0] }}' as var_2,
    '{{ var("variable_3")["value"] }}' as var_3
from
    dual
