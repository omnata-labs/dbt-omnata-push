-- depends_on: {{ ref('omnata_push', 'sfmc_load_tasks') }}
-- depends_on: {{ ref('omnata_push', 'sfmc_load_task_logs') }}
{{
  config(
    materialized='omnata_push',
    app='marketing_cloud',
    operation='data_extension_upload',
    import_type='AddAndUpdate',
    data_extension_name='ContactsFromDbt',
    data_extension_fields = [
      {'Name':'ContactID',   'FieldType':'Number',      'IsPrimaryKey':'true',  'IsRequired':'true'  },
      {'Name':'First Name',  'FieldType':'Text',        'IsPrimaryKey':'false', 'IsRequired':'false' },
      {'Name':'Last Name',   'FieldType':'Text',        'IsPrimaryKey':'false', 'IsRequired':'false' },
      {'Name':'EmailAddress','FieldType':'EmailAddress','IsPrimaryKey':'false', 'IsRequired':'true'  },
      {'Name':'Title',       'FieldType':'Text',        'IsPrimaryKey':'false', 'IsRequired':'false' }],
    force_check=False
  )
}}

select OBJECT_CONSTRUCT('ContactID',CONTACT_NUMBER,
                        'First Name',FIRST_NAME,
                        'Last Name',LAST_NAME,
                        'EmailAddress',EMAIL,
                        'Title',TITLE
                        ) as RECORD
from {{ ref('contacts') }}

{% if var('full-refresh-marketing-cloud')==false %}

  -- this filter will only be applied on an incremental run, to prevent re-sync
  -- of previously successful records
  where RECORD not in (
    select logs.RECORD
    from {{ ref('omnata_push','sfmc_load_task_logs') }} logs
    where logs.load_task_name= '{{ this.name }}'
    and logs.RESULT:"success" = true
  )
  
{% endif %}
