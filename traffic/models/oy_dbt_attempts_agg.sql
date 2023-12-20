{{ config(materialized='table') }}

select 			t.*
				, da.pd_user_id 
				, lm.Master_Account_Name
				, lm.Master_Account_ID 
				, lm.Master_Customer_ID 
				, lm.Main_Sub_Flag 
				, lm.UC_Account_ID_L0 
				, lm.UC_Account_Name_L0 
				, lm.UC_UserName_L0 
				, lm.UC_Account_Country_L0
				, lm.Charging_ID_L0
				, c.destination_country
				, o.operator_name
				, n.network_name
from 			{{ ref('oy_dbt_attempts_agg_stg')}} as t
----------------
left join 		{{ source('standard','dim_fcdr_accounts')}} as da
on 				t.account_id = da.fcdr_account_nk
----------------
left join 		{{ source('analytics','lm_v_master_account_mapping')}} as lm
on 				da.pd_user_id = lm.PD_User_ID_L0 
----------------
left join 		analytics.oy_dbt_dim_countries as c 
on 				t.country_id = c.country_id 
----------------
left join 		(select * from {{ source('analytics','oy_dbt_dim_operators')}} where operator_id_cnt = 1) as o 
on 				t.operator_id = o.operator_id 
----------------
left join 		(select * from {{ source('analytics','oy_dbt_networks')}} where network_id_cnt = 1) as n 
on 				t.network_id = n.network_id 
----------------
where 			1=1