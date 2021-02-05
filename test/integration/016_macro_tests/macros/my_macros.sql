
{% macro do_something2(foo2, bar2) %}

    select
        CAST('{{ foo2 }}' as varchar2(4000 char)) as foo2,
        CAST('{{ bar2 }}' as varchar2(4000 char)) as bar2
    from
        dual

{% endmacro %}


{% macro with_ref() %}

    {{ ref('table_model') }}

{% endmacro %}


{# there is no no default__dispatch_to_nowhere! #}
{% macro dispatch_to_nowhere() %}
	{% set macro = adapter.dispatch('dispatch_to_nowhere') %}
	{{ macro() }}
{% endmacro %}
