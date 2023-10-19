{{config (materialized='view')}} 

select 		*
from 		{{ ref('oy_dbt_consumption_agg')}}
where 		event_type = 'default.sms' or general_ledger_code='Forfeiture Of Package Balance'
            and date_nk >= '2023-01-01'
            and Account_Type in ('Client Account - Open', 'Client Account')