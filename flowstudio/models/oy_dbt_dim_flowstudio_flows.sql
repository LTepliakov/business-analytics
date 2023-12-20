

select 			    f.flow_id
                    , f.flow_version
                    , f.name as flow_name
                    , f.trigger_type
                    , f.created_at as flow_created_at
                    , f.account_id
                    , lm.Master_Customer_ID
                    , lm.Master_Account_ID 
                    , lm.Master_Account_Name 
                    , lm.Main_Sub_Flag 
                    , lm.UC_Account_Name_L0 
                    , lm.Charging_ID_L0 
                    , lm.PD_User_ID_L0 
from 			    {{ source('standard', 'dim_flowstudio_flows') }} as f
left join 		    {{ source('analytics', 'lm_v_master_account_mapping')}} as lm
on 				    f.account_id = lm.UC_Account_ID_L0
where 			    1=1