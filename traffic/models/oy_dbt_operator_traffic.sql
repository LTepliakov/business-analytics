{{config (materialized='table')}}

select 			t.*
                , lmm.Master_Account_ID
				, lmm.Master_Customer_ID
                , lmm.Master_Account_Name
from 			{{ source('analytics','oy_dbt_traffic_operator_agg_stg') }} as t 
----------------
left join 	    {{ source('analytics','lm_v_master_account_mapping')}} as lmm
on   			t.charging_id = lmm.Charging_ID_L0
----------------
where		    1=1