{% macro oracle__check_schema_exists(information_schema, schema) -%}
  {{ return(check_exists_count('check_schema_exist', 'ALL_USERS', 'USERNAME', schema)) }}
{% endmacro %}


{% macro oracle__list_schemas(database) -%}
  {% set sql %}
        SELECT DISTINCT
            username
        FROM
            ALL_USERS
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}


{% macro oracle__create_schema(relation) -%}
  {% call statement('create_schema') %}
    CREATE USER {{ relation.without_identifier() }} IDENTIFIED BY {{ relation.without_identifier() }}
  {% endcall %}
  {% call statement('create_schema_grant') %}
    GRANT ALL PRIVILEGES TO {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}

{% macro oracle__drop_schema(relation) -%}
  {% set drop_query %}
        DROP USER {{ relation.without_identifier() }} CASCADE
  {% endset %}
  {{
    drop_if_exists(
        'drop_schema',
        drop_query,
        'ALL_USERS',
        'USERNAME',
        relation.without_identifier()
    )
  }}
{% endmacro %}

{% macro oracle__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True, auto_begin=False) -%}
    SELECT
      '{{ schema_relation.database }}' AS database,
      table_name AS name,
      owner AS schema,
      'table' AS type
    FROM
        ALL_TABLES
    WHERE
        UPPER(owner) = UPPER('{{ schema_relation.schema }}')
    UNION ALL
    SELECT
      '{{ schema_relation.database }}' AS database,
      view_name AS name,
      owner AS schema,
      'view' AS type
    FROM
        ALL_VIEWS
    WHERE
        UPPER(owner) = UPPER('{{ schema_relation.schema }}')
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro oracle__drop_relation(relation) -%}
  {% set drop_query %}
        DROP {{ relation.type | upper}} {{ relation }}
  {% endset %}
  {{
    drop_if_exists(
        'drop_relation',
        drop_query,
        'ALL_OBJECTS',
        'OWNER',
        relation.schema,
        'OBJECT_NAME',
        relation.identifier,
        'OBJECT_TYPE',
        relation.type
    )
  }}
{% endmacro %}

{% macro drop_table(owner, table_name, type = 'table') -%}
  {{ return(adapter.dispatch('drop_table')(owner, table_name, type)) }}
{% endmacro %}

{% macro oracle__drop_table(owner, table_name, type = 'table') -%}
  {% set drop_query %}
        DROP TABLE {{ owner }}.{{ table_name }}
  {% endset %}
  {{
    drop_if_exists(
        'drop_table',
        drop_query,
        'ALL_OBJECTS',
        'OWNER',
        owner,
        'OBJECT_NAME',
        table_name,
        'OBJECT_TYPE',
        type
    )
  }}
{% endmacro %}


{% macro oracle__rename_relation(from_relation, to_relation) -%}
  {% set objects_schema = from_relation.schema %}
  {% set rename_procedure_name = 'DBT_RENAME__' ~ range(100000) | random %}
  {% set check_exists_result = check_exists(
        'rename_relation_drop_procedure_if_exists',
        'ALL_PROCEDURES',
        'OBJECT_NAME',
        rename_procedure_name,
        'OWNER',
        from_relation.schema
     )
  %}
  {% if not check_exists_result %}
      {% call statement('rename_relation_procedure') -%}
        CREATE PROCEDURE {{ objects_schema }}.{{ rename_procedure_name }}(name VARCHAR2, new_name VARCHAR2) IS
        BEGIN
            EXECUTE IMMEDIATE 'RENAME ' || name || ' TO ' || new_name;
        END;
      {%- endcall %}
  {% endif %}
  {% call statement('rename_relation') -%}
        BEGIN
            {{objects_schema}}.{{ rename_procedure_name }}('{{ from_relation.identifier }}', '{{ to_relation.identifier }}');
        END;
      {%- endcall %}
{% endmacro %}


{% macro oracle__get_columns_in_relation(relation) -%}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        SELECT
            column_name,
            data_type,
            char_length,
            data_precision,
            data_scale
        FROM
            ALL_TAB_COLS
        WHERE
            UPPER(table_name) = UPPER('{{ relation.identifier }}')
            {% if relation.schema %}
            AND UPPER(owner) = UPPER('{{ relation.schema }}')
            {% endif %}
        ORDER BY
            column_id
    {% endcall %}
    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro oracle__create_table_as(temporary, relation, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    {% if temporary %}
        CREATE GLOBAL TEMPORARY TABLE {{ relation }} ON COMMIT PRESERVE ROWS
    {% else %}
        CREATE TABLE {{ relation }}
    {% endif %}
    AS {{ sql }}
{% endmacro %}

{% macro oracle__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  CREATE VIEW {{ relation }}
  {% if 'with' in sql|lower %}
    AS {{ sql }}
  {% else %}
    AS (
        {{ sql }}
    );
  {% endif %}
{% endmacro %}


{% macro oracle__alter_column_type(relation, column_name, new_column_type) -%}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade!)
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') %}
    ALTER TABLE {{ relation }} ADD {{ adapter.quote(tmp_column) }} {{ new_column_type }};
    UPDATE {{ relation }} SET {{ adapter.quote(tmp_column) }} = {{ adapter.quote(column_name) }};
    ALTER TABLE {{ relation }} DROP COLUMN {{ adapter.quote(column_name) }};
    ALTER TABLE {{ relation }} RENAME COLUMN {{ adapter.quote(tmp_column) }} TO {{ adapter.quote(column_name) }}
  {% endcall %}

{% endmacro %}

{% macro oracle__create_columns(relation, columns) %}
  {% for column in columns %}
    {% call statement() %}
      ALTER TABLE {{ relation }} ADD "{{ column.name }}" {{ column.data_type }};
    {% endcall %}
  {% endfor %}
{% endmacro %}

{% macro oracle__current_timestamp() -%}
  LOCALTIMESTAMP
{%- endmacro %}

{% macro oracle__snapshot_string_as_time(timestamp) -%}
    {%- set result = "TO_TIMESTAMP('" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro oracle__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        SELECT * FROM (
            {{ select_sql }}
        ) o__dbt_sbq
        WHERE 0 = 1
        FETCH FIRST 0 ROWS ONLY
    {% endcall %}

    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}


{% macro oracle__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = base_relation.identifier ~ suffix ~ py_current_timestring() %}
    {% do return(base_relation.incorporate(
                                  path={
                                    "identifier": tmp_identifier,
                                    "schema": none,
                                    "database": none
                                  })) -%}
{% endmacro %}

{#
  By using dollar-quoting like this, users can embed anything they want into their comments
  (including nested dollar-quoting), as long as they do not use this exact dollar-quoting
  label. It would be nice to just pick a new one but eventually you do have to give up.
#}
{% macro oracle_escape_comment(comment) -%}
  {% if comment is not string %}
    {% do exceptions.raise_compiler_error('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  q'[{{comment}}]'
{%- endmacro %}

{% macro oracle__alter_relation_comment(relation, relation_comment) -%}
  {% set escaped_comment = oracle_escape_comment(relation_comment) %}
  {% if (relation.type | upper) == 'MATERIALIZED VIEW' %}
    COMMENT ON MATERIALIZED VIEW {{ relation.schema }}.{{ relation.identifier }} IS {{ escaped_comment }}
  {% else %}
    COMMENT ON TABLE {{ relation.schema }}.{{ relation.identifier }} IS {{ escaped_comment }}
  {% endif %}
{% endmacro %}

{% macro oracle__alter_column_comment(relation, column_dict) -%}
    {% for column_name in column_dict %}
        {% set comment = column_dict[column_name]['description'] %}
        {% set escaped_comment = oracle_escape_comment(comment) %}

        comment on column {{ relation.schema }}.{{ relation.identifier }}.{{ column_name }} is {{ escaped_comment }};
  {% endfor %}
{% endmacro %}
