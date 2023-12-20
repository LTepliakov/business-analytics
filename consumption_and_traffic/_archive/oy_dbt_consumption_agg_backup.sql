{{config (materialized='table')}} 

select 	        LAST_DAY(ADD_MONTHS(hour_nk, -1)) + 1 as rep_month
                , ca.event_type
                , ca.general_ledger_code
                , date(ca.hour_nk) as date_nk
                , ca.account_id as charging_id
                , da.pd_user_id
                , da.pd_user_account_name
                , ca.bundle_name
                , ca.bundle_type
                , da.pd_user_country
                , case 	when ca.bundle_name in ('10002202_UAE All Operators', '10002202_mb') then '9598' -- Tamara
                        when ca.bundle_name in ('20000600_UAE All Operators') then '10168' -- Tabby
                        when ca.account_id = 114 then '3436'
                        else null 
                        end as manual_odoo_id
                , case 	when ca.bundle_name in ('10002202_UAE All Operators', '10002202_mb') then 'Tamara FZE' -- Tamara
                        when ca.bundle_name in ('20000600_UAE All Operators') then 'Tabby FZ-LLC' -- Tabby
                        else null 
                        end as manual_odoo_name
                , ca.tariff
                , lmm.Master_Account_ID
                , lmm.Master_Account_Name
                , lmm.Master_Account_Origin
                , tm.hs_team
                , tm.hs_manager
                , tm.tcsm_manager
                , case when sa.Account_Type is null then 'Client Account' else sa.Account_Type end as Account_Type
                , case when top40."Customer Name" is null then null else 'Top 40' end as client_rank
                , ca.country_id
                , ca.operator_id
                , ca.network_name
                , case  when ca.country_id is not null and ca.operator_id is not null and ca.network_name is not null
                        then ca.country_id||'/'||ca.operator_id||'/'||ca.network_name
                        else null
                        end as cost_id
                , ca.current_selling_rate_value as ocs_selling_rate
                , sum(units) as units
                , sum(charge) as charge
                , sum(chargeable_units) as chargeable_units
                , sum(case  when split_part(ca.bundle_name,'_',2)='mb' then charge
                        else ca.charge*ca.current_selling_rate_value
                        end
                ) as Revenue_USD
from 		{{ ref('oy_dbt_fact_sms_consumption_aggregate_stg')}} as ca --aggregate.fact_sms_consumption_aggregate 
left join 	{{ source('standard', 'dim_accounts') }} as da --standard.dim_accounts as da
on 			ca.account_id = da.charging_id
-------
left join 	{{ ref('oy_dbt_lm_mapping') }} as lmm
on 			ca.account_id = lmm.Charging_ID_L0
-------
left join 	{{ source('analytics', 'oy_dbt_odoo_hs_team_manager_distinct')}} as tm 
on 			tm.odoo_id = lmm.Master_Account_ID
-------
left join	{{ source('analytics','oy_dbt_special_accounts')}} as sa
on 			ca.account_id = sa.charging_id
-------
left join 	{{ ref('oy_dbt_top40_clients')}} as top40
on 			lmm.Master_Account_ID = top40."Odoo ID"
-------
group by	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26