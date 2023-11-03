{{ config(materialized='ephemeral')}}

with s as
(
select 		distinct Main_accounts 
from 		{{ source('analytics', 'oy_dbt_seed_DASD_50_parent_child_accounts')}}
)
select 		s.Main_accounts, lm.UC_Account_ID_L0, Charging_ID_L0 
from 		s
left join 	{{ source('analytics', 'lm_v_master_account_mapping')}} as lm
on 			s.Main_accounts = lm.UC_Super_Parent_ID
--where 		lm.UC_Account_ID_L0 is null
order by 	s.Main_accounts