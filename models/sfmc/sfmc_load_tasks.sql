{{ config(
    schema='load_tasks',
    unique_key='job_id',
    materialized='tracking_table')
}}

with template as(
    select null::varchar(100) as job_id,
        null::varchar(255) as load_task_name,
        null::varchar(255) as object_name,
        null::varchar(255) as operation,
        null::timestamp as creation_time,
        null::variant as creation_metadata
)
select * from template where 0=1
