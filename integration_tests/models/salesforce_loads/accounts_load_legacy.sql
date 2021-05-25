-- depends_on: {{ ref('omnata_push','sfdc_load_tasks') }}
-- depends_on: {{ ref('omnata_push','sfdc_load_task_logs') }}
{{
  config(
    materialized='load_task',
    operation='upsert',
    object_name='Account',
    external_id_field='AccountID__c'
  )
}}

select OBJECT_CONSTRUCT('Name',NAME,
                      'AccountID__c',ACCOUNT_ID) as RECORD
from {{ ref('accounts') }}
where 1=1

{% if var('full-refresh-salesforce')==false %}

  -- this filter will only be applied on an incremental run, to prevent re-sync
  -- of previously successful records
  and RECORD:"AccountID__c"::varchar not in (
    select logs.RECORD:"AccountID__c"::varchar 
    from {{ ref('omnata_push','sfdc_load_task_logs') }} logs
    where logs.load_task_name= '{{ this.name }}'
    and logs.RESULT:"success" = true
  )
  
{% endif %}


