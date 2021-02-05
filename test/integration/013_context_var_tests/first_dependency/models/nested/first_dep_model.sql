select
    CAST('{{ var("first_dep_global") }}' AS VARCHAR2({{ var("first_dep_global")|length + 20 }} CHAR)) as first_dep_global,
    CAST('{{ var("from_root_to_first") }}' AS VARCHAR2({{ var("from_root_to_first")|length + 20 }} CHAR)) as from_root
from dual
