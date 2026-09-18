{#- Both catalog macros delegate to the adapter, as dbt-athena does: the
    catalog comes from Glue GetTables (see AthenaAwsClients), one row per
    column in the information_schema shape. A schema-wide information_schema
    query would fail whenever one table in the schema has unreadable Iceberg
    metadata; Glue does not read table data at all. -#}
{% macro athena__get_catalog(information_schema, schemas) -%}
    {{ return(adapter.get_catalog(information_schema, schemas)) }}
{%- endmacro %}


{% macro athena__get_catalog_relations(information_schema, relations) -%}
  {{ return(adapter.get_catalog_by_relations(information_schema, relations)) }}
{%- endmacro %}

{#- Fusion's `default__check_schema_exists` calls
    `information_schema.replace(information_schema_view=...)`, which Fusion
    relations do not implement; every adapter package overrides it. Athena's
    Glue-backed `information_schema.schemata` is case-insensitive in practice
    (everything is stored lowercased), so compare lowercased. -#}
{% macro athena__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True, auto_begin=False) -%}
    select count(*) from information_schema.schemata
    where lower(schema_name) = '{{ schema | lower }}'
    {%- if information_schema.database %}
      and lower(catalog_name) = '{{ information_schema.database | replace('"', '') | lower }}'
    {%- endif %}
  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{% endmacro %}
