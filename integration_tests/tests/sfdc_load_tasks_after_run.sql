
with job_count as (
select count(*) as jobs from {{ ref('omnata_push','sfdc_load_tasks') }}
)
select * from job_count where jobs != 2