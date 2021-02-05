select 1 as id from dual

{% if adapter.already_exists(this.schema, this.identifier) and not should_full_refresh() %}
	where id > (select max(id) from {{this}})
{% endif %}
