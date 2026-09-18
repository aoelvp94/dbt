{#- dbt-athena reads columns from Glue (get_columns_in_relation override).
    The Rust adapter exposes that as get_glue_table_columns, returning the
    information_schema.columns shape sql_convert_columns_in_relation expects.
    It must not call adapter.get_columns_in_relation: Fusion runs this macro
    from that very method. -#}
{% macro athena__get_columns_in_relation(relation) -%}
  {{ return(sql_convert_columns_in_relation(adapter.get_glue_table_columns(relation))) }}
{% endmacro %}

{% macro athena__get_empty_schema_sql(columns) %}
    {%- set col_err = [] -%}
    select
    {% for i in columns %}
      {%- set col = columns[i] -%}
      {%- if col['data_type'] is not defined -%}
        {{ col_err.append(col['name']) }}
      {%- else -%}
        {% set col_name = adapter.quote(col['name']) if col.get('quote') else col['name'] %}
        cast(null as {{ dml_data_type(col['data_type']) }}) as {{ col_name }}{{ ", " if not loop.last }}
      {%- endif -%}
    {%- endfor -%}
    {%- if (col_err | length) > 0 -%}
      {{ exceptions.column_type_missing(column_names=col_err) }}
    {%- endif -%}
{% endmacro %}
