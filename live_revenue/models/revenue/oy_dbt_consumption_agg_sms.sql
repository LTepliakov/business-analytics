{{config (materialized='ephemeral')}} 

select 		*
from 		{{ ref('oy_dbt_consumption_agg')}}
where 		event_type = 'default.sms'
            and date_nk >= '2023-01-01'
            and Account_Type in ('Client Account - Open', 'Client Account')