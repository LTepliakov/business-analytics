

select 			w.widget_id 
				, w.flow_id
				, w.flow_version
				, w.name as widget_name
				, w.type as widget_type
				, w.parent_id
				, w.channel
				, w.created_at as widget_created_at
				, w.account_id
				, lm.Master_Customer_ID
				, lm.Master_Account_ID 
				, lm.Master_Account_Name 
				, lm.Main_Sub_Flag 
				, lm.UC_Account_Name_L0 
				, lm.Charging_ID_L0 
				, lm.PD_User_ID_L0 
from 			{{ source('standard', 'dim_flowstudio_flow_widgets')}} as w
left join 		{{ source('analytics', 'lm_v_master_account_mapping')}}  as lm
on 				w.account_id = lm.UC_Account_ID_L0
where 			1=1