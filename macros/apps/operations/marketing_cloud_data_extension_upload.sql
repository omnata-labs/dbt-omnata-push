{% macro marketing_cloud_data_extension_upload(job_id,load_task_name) -%}
    {%- set operation = config.get('operation') -%}
    {%- set import_type = config.get('import_type') -%}
    {%- set force_check = config.get('force_check') -%}
    {%- set data_extension_fields = config.get('data_extension_fields') -%}
    {%- set data_extension_name = config.get('data_extension_name') -%}
    {%- set omnata_functions_database = var("omnata_functions_database", target.database) -%}
    {%- set omnata_functions_schema = var("omnata_functions_schema", target.schema) -%}
    

    {# -- Store the load job details in the jobs table, including the results of checking the data extension #}
    {% call statement('main') -%}
        insert into {{ ref('omnata_push','sfmc_load_tasks') }} (job_id,load_task_name,object_name,operation,creation_time,creation_metadata)
        select '{{ job_id }}',
               '{{ load_task_name }}',
               '{{ data_extension_name }}',
               '{{ operation }}',
               current_timestamp(),
               "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_DATA_EXTENSION_MANAGE(PARSE_JSON('{"operation":"ensure_exists",
                    "extension_name":"{{ data_extension_name }}",
                    "force":"{{ force_check }}",
                    "extension_fields": {{ data_extension_fields | tojson }}
                }'))
    {%- endcall %}

    {# -- Load the data in batches, waiting for the result so that we can store it at the record level #}
    {% call statement('main') -%}
        insert into {{ ref('omnata_push','sfmc_load_task_logs') }}
        with load_source as (
            {{ sql }}
        )
        select '{{ job_id }}' as job_id,
                UUID_STRING() as job_log_entry_id,
                '{{ load_task_name }}',
                '{{ data_extension_name }}',
                '{{ operation }}',
                load_source.record,
                "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_DATA_EXTENSION_IMPORT(PARSE_JSON('{"name":"{{ data_extension_name }}","operation":"{{ import_type }}"}'),
                                           load_source.record) as result 
        from load_source
    {%- endcall %}

    {{ adapter.commit() }}

{%- endmacro %}