-- depends_on: {{ ref('omnata_push','sfdc_load_tasks') }}
-- depends_on: {{ ref('omnata_push','sfdc_load_task_logs') }}
{{
  config(
    materialized='load_task',
    operation='upsert',
    object_name='Daily_Transaction__c',
    external_id_field='Duplicate_Key__c'
  )
}}

select OBJECT_CONSTRUCT('Name',ACCOUNT_ID||'_'||TRANSACTIONS_DATE,
                      'Duplicate_Key__c',ACCOUNT_ID||'_'||TRANSACTIONS_DATE,
                      'Total_Transactions_Sum__c',GTV_DAILY,
                      'Total_Revenue_Sum__c',NTR_DAILY,
                      'Transactions_Date__c',TRANSACTIONS_DATE,
                      'Account__r',OBJECT_CONSTRUCT('AccountID__c',ACCOUNT_ID)) as RECORD
from {{ ref('transactions_daily') }}

{% if var('full-refresh-salesforce')==false %}

  -- this filter will only be applied on an incremental run, to prevent re-sync
  -- of previously successful records
  where RECORD:"Duplicate_Key__c"::varchar not in (
    select logs.RECORD:"Duplicate_Key__c"::varchar 
    from {{ ref('omnata_push','sfdc_load_task_logs') }} logs
    where logs.load_task_name= '{{ this.name }}'
    and logs.RESULT:"success" = true
  )
  
{% endif %}

