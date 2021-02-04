{{ config(
    materialized="incremental"
) }}

with aggregated_transactions as (
select ACCOUNT_ID,
    TRANSACTION_DATETIME::date as TRANSACTIONS_DATE,
    SUM(TRANSACTION_AMOUNT) as GTV_DAILY,
    SUM(REVENUE_AMOUNT) as NTR_DAILY
from {{ ref('transactions') }}
group by ACCOUNT_ID,TRANSACTION_DATETIME::date
)
select * from aggregated_transactions
where TRANSACTIONS_DATE::date < current_date()

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
    and TRANSACTIONS_DATE > (select max(TRANSACTIONS_DATE) from {{ this }})

{% endif %}
