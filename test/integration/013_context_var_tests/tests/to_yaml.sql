
{% set simplest = (toyaml({'a': 1}) == 'a: 1\n') %}
{% set default_sort = (toyaml({'b': 2, 'a': 1}) == 'b: 2\na: 1\n') %}
{% set unsorted = (toyaml({'b': 2, 'a': 1}, sort_keys=False) == 'b: 2\na: 1\n') %}
{% set sorted = (toyaml({'b': 2, 'a': 1}, sort_keys=True) == 'a: 1\nb: 2\n') %}
{% set default_results = (toyaml({'a': adapter}, 'failed') == 'failed') %}

(select 'simplest' as name from dual {% if simplest %}fetch first 0 rows only{% endif %})
union all
(select 'default_sort' as name from dual {% if simplest %}fetch first 0 rows only{% endif %})
union all
(select 'unsorted' as name from dual {% if simplest %}fetch first 0 rows only{% endif %})
union all
(select 'sorted' as name from dual {% if simplest %}fetch first 0 rows only{% endif %})
union all
(select 'default_results' as name from dual {% if simplest %}fetch first 0 rows only{% endif %})

