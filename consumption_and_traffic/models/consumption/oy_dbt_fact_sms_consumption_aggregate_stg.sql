{{
  config(
    materialized = 'table',
    )
}}

with base as
(
select          date(hour_nk) as date_nk 
                , *
                , case  when split_part(bundle_name,'_',2)='mb' then 'mb'
                        else 'package'
                        end as bundle_type
from            {{ source('aggregate', 'fact_sms_consumption_aggregate')}}
)
select          ca.*
                , case when ca.bundle_type = 'mb' then units else charge end as chargeable_units
                , da.pd_user_id
                , da.pd_user_account_name
                , da.pd_user_country
                , lmm.Master_Customer_ID
                , lmm.Master_Account_ID
                , lmm.Master_Account_Name
                , lmm.Master_Account_Origin
                , tm.hs_team
                , tm.hs_manager
                , tm.tcsm_manager
                , case when sa.Account_Type is null then 'Client Account' else sa.Account_Type end as Account_Type
                , case when top40."Customer Name" is null then null else 'Top 40' end as client_rank
from            base as ca
---------------
left join 	    {{ source('standard', 'dim_accounts') }} as da
on 			    ca.account_id = da.charging_id
---------------
left join 	    {{ source('analytics', 'lm_v_master_account_mapping')}} as lmm
on 			    ca.account_id = lmm.Charging_ID_L0
---------------
left join 	    {{ source('analytics', 'oy_dbt_odoo_hs_team_manager_distinct')}} as tm 
on 			    tm.odoo_id = lmm.Master_Account_ID
---------------
left join	    {{ source('analytics','oy_dbt_special_accounts')}} as sa
on 			    ca.account_id = sa.charging_id
---------------
left join 	    {{ ref('oy_dbt_top40_clients')}} as top40
on 			    lmm.Master_Account_ID = top40."Odoo ID"
---------------
where           lmm.Master_Account_Name <> 'migration traffic'