{{ config(materialized='view')}}

with s as
(
select 		distinct Main_accounts, Subaccounts
from 		{{ source('analytics', 'oy_dbt_seed_DASD_50_parent_child_accounts_2')}}
)
, lm as
(
select distinct UC_Super_Parent_ID, UC_Account_ID_L0, Charging_ID_L0 from {{ source('analytics', 'lm_v_master_account_mapping')}}
)
select 		s.Main_accounts, s.Subaccounts
            , lm.Charging_ID_L0
            , lm1.Charging_ID_L0 as Charging_ID_L0_1
from 		s
------------
left join 	lm as lm
on 			s.Main_accounts = lm.UC_Super_Parent_ID
------------
left join 	lm as lm1
on 			s.Subaccounts = lm1.UC_Account_ID_L0
------------
--where 		lm.UC_Account_ID_L0 is null
order by 	s.Main_accounts

