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
        ,data_indexed as(
        select row_number() over (partition by null order by null) as row_index,
        (row_index/100)::int as batch,
        record
        from load_source
        )
        ,data_staged as(
        select "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_STAGE_DATA(array_agg(array_construct(row_index,record))) as staged_result
        from data_indexed
        group by batch
        )
        ,data_imported as(
            select "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_DE_IMPORT(
                    PARSE_JSON('{"name":"{{ data_extension_name }}","operation":"{{ import_type }}"}'),
                    any_value(staged_result)
                ) as import_result
            from data_staged
        ),
        data_import_result as(
        select any_value(import_result) as import_result_output
        from data_imported
        )
        select '{{ job_id }}' as job_id,
                UUID_STRING() as job_log_entry_id,
                '{{ load_task_name }}',
                '{{ data_extension_name }}',
                '{{ operation }}',
                record,
                "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_FETCH_RESULTS(import_result_output,row_index) as result
        from data_indexed,data_import_result
    {%- endcall %}

    {{ adapter.commit() }}

{%- endmacro %}