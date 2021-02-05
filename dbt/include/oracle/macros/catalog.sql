
{% macro oracle__get_catalog(information_schema, schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}
    {% set database = information_schema.database %}

    SELECT
        '{{ database }}' AS "table_database",
        objects.owner AS "table_schema",
        objects.table_name AS "table_name",
        objects.type AS "table_type",
        descriptions.COMMENTS as "table_comment",
        columns.column_name as "column_name",
        columns.column_id as "column_index",
        columns.data_type as "column_type",
        col_descriptions.comments as "column_comment",
        objects.owner as "table_owner"
    FROM
        (
            SELECT
                owner,
                table_name,
                'BASE TABLE' AS type,
                "TEMPORARY" AS temp_table
            FROM
                ALL_TABLES
            UNION ALL
            SELECT
                owner,
                view_name,
                'VIEW' AS type,
                'N' AS temp_view
            FROM
                ALL_VIEWS
        ) objects
        INNER JOIN ALL_TAB_COMMENTS descriptions ON (descriptions.owner = objects.owner AND descriptions.table_name = objects.table_name)
        INNER JOIN ALL_TAB_COLUMNS columns ON (columns.owner = objects.owner AND columns.table_name = objects.table_name) -- only user defined columns
        INNER JOIN ALL_COL_COMMENTS col_descriptions ON (col_descriptions.owner = columns.owner AND col_descriptions.table_name = columns.table_name AND col_descriptions.column_name = columns.column_name)
    WHERE (
        {%- for schema in schemas -%}
          UPPER(objects.owner) = UPPER('{{ schema }}'){%- if not loop.last %} OR {% endif -%}
        {%- endfor -%}
      )
      AND objects.temp_table = 'N' -- not temporary
    ORDER BY
        objects.owner,
        objects.table_name,
        columns.column_id

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}
{% endmacro %}