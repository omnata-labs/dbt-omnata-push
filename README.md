# omnata-push

## What is this package?

This dbt package contains pre-built models and macros for using [Omnata Push](https://omnata.com) in a dbt project.

Omnata Push is a commercial offering that provides native data loading from within your Snowflake data warehouse, via External Functions.

## How do I get started?

### Omnata installation

First, sign up to Omnata at (https://portal.omnata.com).

After creating an Omnata Push integration, you will be provided with External Function definitions to run in your Snowflake account, along with a dbt model generator to use with this package.

### dbt project setup

1) Add the omnata-push package as a dependancy in your `packages.yml`:

```

packages:
  - git: "https://github.com/omnata-labs/dbt-omnata-push.git"
    revision: 0.5.0

```

2) run `dbt deps`.

3) Add the following vars to your dbt_project.yml:

```

vars:
  full-refresh-salesforce: false
  full-refresh-marketing-cloud: false
  drop-omnata-task-tables: false
  
```

### Configuring a load task

Load task definitions are generated in the Omnata Portal, and look like the following (example shows Salesforce):

```
-- depends_on: {{ ref('omnata_push','sfdc_load_tasks') }}
-- depends_on: {{ ref('omnata_push','sfdc_load_task_logs') }}
{{
  config(
    materialized='omnata_push',
    app='salesforce',
    operation='bulk_load',
    load_type='upsert',
    object_name='Account',
    external_id_field='AccountID__c'
  )
}}

select OBJECT_CONSTRUCT('Name',NAME,
                      'AccountID__c',ACCOUNT_ID) as RECORD
from {{ ref('accounts') }}

```

Salesforce configuration parameters are as follows:
- `materialized`: always set this to "omnata_push", this tells dbt to load the data into Salesforce rather than create a table/view
- `operation`: The Salesforce Bulk API operation type, one of ('delete','hardDelete','insert','update','upsert'). upsert is the most common and easily configured type, since you can re-run flexibly without having to manage side effects.
- `object_name`: The name of the Salesforce object
- `external_id_field`: Required for upsert operations, defines which field is used to identify records. This field must be marked as External within Salesforce. If defined, this field must be included in the RECORD field of the model definition.
- `serial_load`: Set to true to instruct Salesforce to process batches in serial mode. This will significatly impact load performance, so only enable this if you are experiencing errors due to database contention.

The query must contain a single field, named RECORD. For insert, update, or upsert operations, include any Salesforce fields you like. For delete and hardDelete operations, you must pass in just an "Id" field containing the Salesforce record ID (you may need to retrieve these from the job logs if you aren't already syncing Salesforce data to Snowflake).

The two commented lines at the top are required, for the compilation to work.

### Task history tables

This package automatically creates long-lived tables for tracking load history:

#### Salesforce
- `sfdc_load_tasks`: Contains a record for every Salesforce bulk load job created.
- `sfdc_load_task_logs`: Contains a record for every record provided to a Salesforce bulk load job.
#### Marketing Cloud
- `sfmc_load_tasks`: Contains a record for every Marketing Cloud import performed.
- `sfmc_load_task_logs`: Contains a record for every record provided to a Marketing Cloud import.

For example, if you include 1000 rows in your Salesforce load task, afterwards there will be a single record in `sfdc_load_tasks` and 1000 records in `sfdc_load_task_logs`.

These tables use a special materialization called `tracking_table`, which is immune to the standard `--full-refresh` flag.

The location of these tables can be overriden in your dbt_project.yml file like so:
```
models:
  omnata_push:
    sfdc:
      +database: my_other_database
      +schema: my_other_schema
    sfmc:
      +database: my_other_database
      +schema: my_other_schema
```

If you need to drop these tables completely and recreate, pass in the `drop-omnata-task-tables` flag like so:
```
dbt run --target my_target --vars 'drop-omnata-task-tables: true'
```
You should only need to do this under instruction from Omnata staff.

### Managing multiple app environments

One of the many benefits of dbt is that you develop on branches of code that execute in different target environments. This means you can merge a code change knowing that it yielded the expected result already.

Omnata Push supports this model by allowing you to target different apps via different External Functions, which can be selected based on the dbt target.

By default, dbt-omnata-push expects the External Functions to exist in the database and schema defined in the current target, but you can override this with `project.yml` vars like so:

```
vars:
  omnata_functions_database: OMNATA_FUNCTIONS
  omnata_functions_schema: |
      {%- elif target.name == "prod"  -%} PRODUCTION
      {%- else -%} SANDBOX
      {%- endif -%}
```

For example, if you have a production Salesforce environment as well as a sandbox, simply create the following
### Incremental/partial loads

Instead of using the standard `is_incremental` approach, instead use the `full-refresh-salesforce` flag to narrow down which records to include in the load.

For example, you can reference the `sfdc_load_task_logs` table to ignore previously successful loads like so:
```
-- depends_on: {{ ref('omnata_push','sfdc_load_tasks') }}
-- depends_on: {{ ref('omnata_push','sfdc_load_task_logs') }}
{{
  config(
    materialized='omnata_push',
    app='salesforce',
    operation='bulk_load',
    load_type='upsert',
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

```

### What else can Omnata do?

Omnata is changing the way that data integration works, by removing complex middleware and providing native capabilities to your existing apps and data warehouses.

To find out more or to contact us, visit our [website](http://omnata.com).
