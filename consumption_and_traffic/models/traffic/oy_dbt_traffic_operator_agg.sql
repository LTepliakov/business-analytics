{{config (materialized='table')}}

select 		    t.date_hour::DATE as date_nk
                , t.charging_id
                , lmm.Master_Account_ID
                , lmm.Master_Account_Name
                , t.destination_country
                , t.operator_name
                , t.network_name
                , t.country_id
                , t.operator_id
                , t.country_id||'/'||t.operator_id||'/'||t.network_name  as cost_id
                , t.event_status
                , sum(t.units) as units
from 		    {{ source('aggregate', 'fact_sms_traffic_operator_aggregate') }} as t 
----------------
left join 	    {{ ref('oy_dbt_lm_mapping')}} as lmm
on   			t.charging_id = lmm.Charging_ID_L0
----------------
where		    1=1
                and t.event_status in ('FAILED','FAILED_ESME','SUCCESS_ESME')
group by 	    1,2,3,4,5,6,7,8,9,10,11