{{ config(materialized='table')}}

select 			w.*
				, lm.Master_Customer_ID
				, lm.Master_Account_ID 
				, lm.Master_Account_Name 
				, lm.Main_Sub_Flag 
				, lm.UC_Account_Name_L0 
				, lm.Charging_ID_L0 
				, lm.PD_User_ID_L0 
from 			{{ source('standard', 'fact_flowstudio_flow_executions') }} as w
left join 		{{ source('analytics', 'lm_v_master_account_mapping') }} as lm
on 				w.account_id = lm.UC_Account_ID_L0
where 			1=1