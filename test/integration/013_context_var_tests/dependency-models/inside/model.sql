select
	CAST('{{ var("first_dep_override") }}' AS VARCHAR2({{ var("first_dep_override")|length + 20 }} CHAR)) as first_dep_global,
	CAST('{{ var("from_root_to_root") }}'AS VARCHAR2({{ var("from_root_to_root")|length + 20 }} CHAR)) as from_root
from dual