{% macro marketing_cloud_data_extension_upload(job_id,load_task_name) -%}
    {%- set operation = config.get('operation') -%}
    {%- set import_type = config.get('import_type') -%}
    {%- set force_check = config.get('force_check') -%}
    {%- set encrypted_load = config.get('encrypted',default=False) -%}
    {%- set gpg_public_key = config.get('gpg_public_key') -%}
    {%- set data_extension_fields = config.get('data_extension_fields') -%}
    {%- set data_extension_name = config.get('data_extension_name') -%}
    {%- set data_extension_path = config.get('data_extension_path',None) -%}
    {%- set data_extension_properties = config.get('data_extension_properties',None) -%}
    {%- set file_location_external_key = config.get('file_location_external_key',default='ExactTarget Enhanced FTP') -%}
    {%- set omnata_functions_database = var("omnata_functions_database", target.database) -%}
    {%- set omnata_functions_schema = var("omnata_functions_schema", target.schema) -%}
    {%- set temp_table_database = var("temp_table_database", generate_database_name()) -%}
    {%- set temp_table_schema = var("temp_table_schema", generate_schema_name(custom_schema_name=None)) -%}
    {%- set temp_table = 'temp_'+omnata_push.random_int(10) -%}

    {# -- Store the load job details in the jobs table, including the results of checking the data extension #}
    {% call statement('main') -%}
        create temp table "{{ temp_table_database }}"."{{ temp_table_schema }}".{{ temp_table }} as(
            select "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_DATA_EXTENSION_MANAGE(PARSE_JSON('{"operation":"ensure_exists",
                    "extension_name":"{{ data_extension_name }}",
{%- if data_extension_path %}
                    "extension_path":"{{ data_extension_path }}",        
{% endif -%}
{%- if data_extension_properties %}
                    "extension_properties":{{ data_extension_properties | tojson }},        
{% endif -%}
                    "force":"{{ force_check }}",
                    "extension_fields": {{ data_extension_fields | tojson }}
                }')) as metadata_creation_result
        )
    {%- endcall %}

    {# -- Load the data in batches, waiting for the result so that we can store it at the record level #}
    {% call statement('main') -%}
        insert all
        when row_number=1 then
            into {{ ref('omnata_push','sfmc_load_tasks') }} (job_id, load_task_name,object_name,operation,creation_time,creation_metadata) values (job_id, load_task_name,object_name,operation,creation_time,creation_metadata)
            into {{ ref('omnata_push','sfmc_load_task_logs') }} (job_id, job_log_entry_id,load_task_name,object_name,operation,record,result) values (job_id, job_log_entry_id,load_task_name,object_name,operation,record,result)
        else
            into {{ ref('omnata_push','sfmc_load_task_logs') }} (job_id, job_log_entry_id,load_task_name,object_name,operation,record,result) values (job_id, job_log_entry_id,load_task_name,object_name,operation,record,result)

        with load_parameters as(
        -- This section determines the data import settings
        select PARSE_JSON('{"name":"{{ data_extension_name }}","operation":"{{ import_type }}","encrypted": {{ encrypted_load }}, "file_location_external_key": "{{ file_location_external_key }}" }') as import_parameters, metadata_creation_result
        from "{{ temp_table_database }}"."{{ temp_table_schema }}".{{ temp_table }}
        )
        ,load_source as(
        -- This section determines which data is uploaded to Marketing Cloud, and the field names
            {{ sql }}
        -- --------------------------------------------------------------
        )
        {%- if not encrypted_load %}
            ,data_indexed as( -- assign row numbers to match results
            select row_number() over (partition by null order by null) as row_number,
            (row_number/100)::int as batch_number,
            record
            from load_source
            ),data_staged as( -- batch records for efficiency, stage the data for upload
            select "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_STAGE_DATA(array_agg(array_construct(row_number,record))) as staged_result
            from data_indexed
            group by batch_number
            ),staged_data_result as(
            select any_value(staged_result) as staged_query_id
            from data_staged
            ),data_imported as( -- perform the import
            select staged_query_id,"{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_AWAIT_RESULTS_POLL("{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_DE_IMPORT(import_parameters,staged_query_id)) as import_result
            from staged_data_result,load_parameters
            ),import_results as( -- retrieve the results
            select 
                staged_query_id as job_id,
                '{{ load_task_name }}' as load_task_name,
                UUID_STRING() as job_log_entry_id,
                '{{ data_extension_name }}' as object_name,
                '{{ import_type }}' as operation,
                current_timestamp() as creation_time,
                metadata_creation_result as creation_metadata,
                row_number,
                record,
                "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_FETCH_RESULTS(staged_query_id,row_number) as result
            from data_indexed,data_imported,load_parameters
            where import_result=true
            )
            select * from import_results
        {% else %}
            ,gpg_parameters as(
            select * from table(OMNATA_FUNCTIONS.PRODUCTION.GPG_PARAMS('{{ gpg_public_key }}'))
            )
            -- every input row is assigned a unique, sequential row number, starting from 0
            ,data_indexed as(select row_number() over (partition by null order by null)-1 as original_row_number, record from load_source)
            -- convert JSON to CSV
            ,data_csv as(select * from data_indexed,table(OMNATA_FUNCTIONS.PRODUCTION.JSON_TO_CSV(original_row_number::double,record::variant)))
            -- create a GPG file payload
            ,wrapped_input as(select data_csv.row_number as csv_row_number,wrapped_table.* from data_csv, table(OMNATA_FUNCTIONS.PRODUCTION.GPG_FILE_WRAPPER('file',csv) over (partition by null order by data_csv.row_number)) AS wrapped_table order by row_number asc)
            -- encrypt the GPG payload
            ,encrypted_input as(select encrypted_table.* from wrapped_input,gpg_parameters, table(OMNATA_FUNCTIONS.PRODUCTION.GPG_ENCRYPT(gpg_parameters.GPG_PREFIX,gpg_parameters.KEY_ID,gpg_parameters.SESSION_KEY,CONTENTS) over (partition by null order by wrapped_input.row_number)) as encrypted_table order by encrypted_table.row_number asc)
            -- package the encrypted payload as a GPG message
            ,gpg_message as(select packaged_table.*,(encrypted_input.row_number/100)::int as batch_number from encrypted_input,gpg_parameters, table(OMNATA_FUNCTIONS.PRODUCTION.GPG_PACKAGE_MESSAGE(gpg_parameters.GPG_PREFIX,gpg_parameters.KEY_ID,gpg_parameters.PUBLIC_KEY,gpg_parameters.SESSION_KEY,ENCR) over (partition by null order by encrypted_input.row_number)) as packaged_table)
            -- stage the data for upload
            ,data_staged as(select batch_number,"{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_STAGE_DATA(array_agg(array_construct(gpg_message.row_number,GPG_MESSAGE_CONTENTS))) as staged_result from gpg_message group by batch_number)
            ,staged_data_result as(select any_value(staged_result) as staged_query_id from data_staged)
            -- perform the import and wait for completion
            ,data_imported as(select staged_query_id, "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_AWAIT_RESULTS_POLL("{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_DE_IMPORT(import_parameters,staged_query_id)) as import_result from staged_data_result,load_parameters)
            -- retrieve the results, matching to original row index
            ,import_results as(
                select staged_query_id as job_id,
                '{{ load_task_name }}' as load_task_name,
                UUID_STRING() as job_log_entry_id,
                '{{ data_extension_name }}' as object_name,
                '{{ import_type }}' as operation,
                current_timestamp() as creation_time,
                metadata_creation_result as creation_metadata,
                original_row_number as row_number,
                record,
                "{{ omnata_functions_database }}"."{{ omnata_functions_schema }}".SFMC_FETCH_RESULTS(staged_query_id,row_number) as result
            from data_indexed,data_imported,load_parameters
            where import_result=true)
            select * from import_results
        {% endif -%}
    {%- endcall %}

    {{ adapter.commit() }}

{%- endmacro %}