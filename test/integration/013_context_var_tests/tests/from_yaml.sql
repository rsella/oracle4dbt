{% set simplest = (fromyaml('a: 1') == {'a': 1}) %}
{% set nested_data %}
a:
  b:
   - c: 1
     d: 2
   - c: 3
     d: 4
{% endset %}
{% set nested = (fromyaml(nested_data) == {'a': {'b': [{'c': 1, 'd': 2}, {'c': 3, 'd': 4}]}}) %}

(select 'simplest' as name from dual {% if simplest %}fetch first 0 rows only{% endif %})
union all
(select 'nested' as name from dual {% if simplest %}fetch first 0 rows only{% endif %})
