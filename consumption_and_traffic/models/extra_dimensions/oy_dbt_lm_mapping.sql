{{ config(materialized = 'ephemeral') }}

select 		Charging_ID_L0
            , COALESCE(manual_odoo_id, Master_Account_ID) as Master_Account_ID
            , COALESCE(manual_odoo_name, Master_Account_Name) as Master_Account_Name
            , Master_Account_Origin
from 		{{ source('analytics', 'lm_v_master_account_mapping')}}
where       Master_Account_Name <> 'migration traffic'