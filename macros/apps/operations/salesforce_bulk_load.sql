{% macro salesforce_bulk_load(job_id,load_task_name) -%}
    {%- set operation = config.require('operation') -%}
    {%- set load_type = config.require('load_type') -%}
    {%- set load_task_name = this.name -%}
    {%- set object_name = config.require('object_name') -%}
    {%- set external_id_field = config.require('external_id_field') -%}
    {%- set serial_load = config.get('serial_load', default=False) -%}
    {%- set temp_table = 'temp_'+omnata_push.random_int(10) -%}
    
    {# -- Call the Omnata API to create a load job #}
    {% set query_text %}
    create temp table {{ temp_table }} as(
        select SFDC_BULK_API_CREATE_JOB('{{ load_type }}','{{ object_name }}',{{ serial_load }},'{{ external_id_field }}') as METADATA
    )
    {% endset %}
    {% set job_creation_result = run_query(query_text) %}

    {# -- Store the load job details in the jobs table #}
    {% call statement('main') -%}
        insert into {{ ref('omnata_push','sfdc_load_tasks') }} (job_id,load_task_name,object_name,operation,external_id_field,creation_time,creation_metadata)
        select METADATA:"id",
            '{{ load_task_name }}',
            '{{ object_name }}',
            '{{ load_type }}',
            '{{ external_id_field }}',
            current_timestamp(),
            METADATA
        from {{ temp_table }};
    {%- endcall %}

    {# -- Load the data in batches, waiting for the result so that we can store it at the record level #}
    {% call statement('main') -%}
        insert into {{ ref('omnata_push','sfdc_load_task_logs') }}
        with load_source as (
            {{ sql }}
        )
        select METADATA:"id" as job_id,
                UUID_STRING() as job_log_entry_id,
                '{{ load_task_name }}',
                '{{ object_name }}',
                '{{ load_type }}',
                '{{ external_id_field }}',
                load_source.record,
                SFDC_BULK_API_LOAD_BATCH(METADATA:"id",load_source.RECORD,true) as result 
        from load_source,{{ temp_table }};
    {%- endcall %}

    {# -- Close off the job as a courtesy to Salesforce #}
    {% call statement('main') -%}
        update {{ ref('omnata_push','sfdc_load_tasks') }} load_tasks
        set close_metadata = SFDC_BULK_API_CLOSE_JOB(METADATA:"id",true)
        from {{ temp_table }}
        where load_tasks.job_id = METADATA:"id";
    {%- endcall %}


    {{ adapter.commit() }}

{%- endmacro %}