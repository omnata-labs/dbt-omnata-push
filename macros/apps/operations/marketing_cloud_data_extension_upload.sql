{% macro marketing_cloud_data_extension_upload(job_id,load_task_name) -%}
    {%- set operation = config.get('operation') -%}
    {%- set import_type = config.get('import_type') -%}
    {%- set force_check = config.get('force_check') -%}
    {%- set data_extension_fields = config.get('data_extension_fields') -%}
    {%- set data_extension_name = config.get('data_extension_name') -%}
    {%- set omnata_functions_database = var("omnata_functions_database", target.database) -%}
    {%- set omnata_functions_schema = var("omnata_functions_schema", target.schema) -%}
    {%- set temp_table = 'temp_'+omnata_push.random_int(10) -%}

    {# -- Store the load job details in the jobs table, including the results of checking the data extension #}
    {% call statement('main') -%}
        create temp table {{ temp_table }} as(
            select "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_DATA_EXTENSION_MANAGE(PARSE_JSON('{"operation":"ensure_exists",
                    "extension_name":"{{ data_extension_name }}",
                    "force":"{{ force_check }}",
                    "extension_fields": {{ data_extension_fields | tojson }}
                }')) as metadata_creation_result
        )
    {%- endcall %}

    {# -- Load the data in batches, waiting for the result so that we can store it at the record level #}
    {% call statement('main') -%}
        insert all
        when row_index=1 then
            into {{ ref('omnata_push','sfmc_load_tasks') }} (job_id, load_task_name,object_name,operation,creation_time,creation_metadata) values (job_id, load_task_name,object_name,operation,creation_time,creation_metadata)
            into {{ ref('omnata_push','sfmc_load_task_logs') }} (job_id, job_log_entry_id,load_task_name,object_name,operation,record,result) values (job_id, job_log_entry_id,load_task_name,object_name,operation,record,result)
        else
            into {{ ref('omnata_push','sfmc_load_task_logs') }} (job_id, job_log_entry_id,load_task_name,object_name,operation,record,result) values (job_id, job_log_entry_id,load_task_name,object_name,operation,record,result)

        with parameters as(
        -- This section determines the data import settings
        select PARSE_JSON('{"name":"{{ data_extension_name }}","operation":"{{ import_type }}"}') as import_parameters, metadata_creation_result
        from {{ temp_table }}
        )
        ,load_source as(
        -- This section determines which data is uploaded to Marketing Cloud, and the field names
            {{ sql }}
        -- --------------------------------------------------------------
        ),data_indexed as( -- assign row numbers to match results
        select row_number() over (partition by null order by null) as row_index,
        (row_index/100)::int as batch_number,
        record
        from load_source
        ),data_staged as( -- batch records for efficiency, stage the data for upload
        select "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_STAGE_DATA(array_agg(array_construct(row_index,record))) as staged_result
        from data_indexed
        group by batch_number
        ),staged_data_result as(
        select any_value(staged_result) as staged_query_id
        from data_staged
        ),data_imported as( -- perform the import
        select staged_query_id,"{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_AWAIT_RESULTS_POLL("{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_DE_IMPORT(import_parameters,staged_query_id)) as import_result
        from staged_data_result,parameters
        ),import_results as( -- retrieve the results
        select 
            staged_query_id as job_id,
            '{{ load_task_name }}' as load_task_name,
            UUID_STRING() as job_log_entry_id,
            '{{ data_extension_name }}' as object_name,
            '{{ import_type }}' as operation,
            current_timestamp() as creation_time,
            metadata_creation_result as creation_metadata,
            row_index,
            record,
            SFMC_FETCH_RESULTS(staged_query_id,row_index) as result
        from data_indexed,data_imported,parameters
        where import_result=true
        )
        select * from import_results
    {%- endcall %}

    {{ adapter.commit() }}

{%- endmacro %}