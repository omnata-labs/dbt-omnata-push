{# here for backwards compatibility #}
{% macro incremental_upsert(tmp_relation, target_relation, unique_key=none, statement_name="main") %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {{ get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns) }}
{%- endmacro %}
