{{ config(
    schema='load_tasks',
    unique_key='job_log_entry_id',
    materialized='tracking_table')
}}

with template as(
    select null::varchar(100) as job_id,
        null::varchar(100) as job_log_entry_id,
        null::varchar(255) as load_task_name,
        null::varchar(255) as object_name,
        null::varchar(255) as operation,
        null::varchar(255) as external_id_field,
        null::variant as record,
        null::variant as result
)
select * from template where 0=1
