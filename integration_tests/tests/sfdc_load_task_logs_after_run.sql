
with job_count as (
select count(*) as jobs from {{ ref('omnata_push','sfdc_load_task_logs') }}
)
select * from job_count where jobs != 9700