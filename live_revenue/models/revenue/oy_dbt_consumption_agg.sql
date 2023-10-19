{{config (materialized='view')}} 

select 		LAST_DAY(ADD_MONTHS(hour_nk, -1)) + 1 as rep_month
            , ca.event_type
            , ca.general_ledger_code
            , date(ca.hour_nk) as date_nk
            , ca.account_id as charging_id
            , da.pd_user_id
            , da.pd_user_account_name
            , ca.bundle_name
            , da.pd_user_country
            , case 	when split_part(trim(ca.bundle_name), '_', 2)='mb' then 'mb' else 'package' end as bundle_type
            , case 	when ca.bundle_name in ('10002202_UAE All Operators', '10002202_mb') then '9598' -- Tamara
                    when ca.bundle_name in ('20000600_UAE All Operators') then '10168' -- Tabby
                    when ca.account_id = 114 then '3436' -- Nahdi Medical Company Guest Care, missing erp_id in standard.dim_accounts. When Thomas upload Batch 5 then delete this line of code
                    else null 
                end as manual_odoo_id
            , case 	when ca.bundle_name in ('10002202_UAE All Operators', '10002202_mb') then 'Tamara FZE' -- Tamara
                    when ca.bundle_name in ('20000600_UAE All Operators') then 'Tabby FZ-LLC' -- Tabby
                    else null 
                end as manual_odoo_name
            , ca.tariff
            , ca.current_selling_rate_value
            , lmm.Master_Account_ID
            , lmm.Master_Account_Name
            , lmm.Master_Account_Origin
            , tm.hs_team
            , tm.hs_manager
            , tm.tcsm_manager
            , case when sa.Account_Type is null then 'Client Account' else sa.Account_Type end as Account_Type
            , case when top40."Customer Name" is null then null else 'Top 40' end as client_rank
            , sum(units) as units, sum(charge) as charge
from 		{{ source('aggregate', 'fact_sms_consumption_aggregate')}} as ca --aggregate.fact_sms_consumption_aggregate 
left join 	{{ source('standard', 'dim_accounts') }} as da --standard.dim_accounts as da
on 			ca.account_id = da.charging_id
-------
left join 	(	
				select 		Charging_ID_L0
							, COALESCE(manual_odoo_id, Master_Account_ID) as Master_Account_ID
							, COALESCE(manual_odoo_name, Master_Account_Name) as Master_Account_Name
							, Master_Account_Origin
				from 		{{ source('analytics', 'lm_v_master_account_mapping')}} --analytics.lm_v_master_account_mapping
			) as lmm
on 			ca.account_id = lmm.Charging_ID_L0
-------
left join 	{{ source('analytics', 'oy_dbt_odoo_hs_team_manager_distinct')}} as tm 
on 			tm.odoo_id = lmm.Master_Account_ID
-------
left join	{{ ref('oy_dbt_special_accounts')}} as sa
on 			ca.account_id = sa.charging_id
-------
left join 	(
				select 		* 
				from 		{{ ref('oy_dbt_sales_plans_gsheet')}} --sandbox.oy_sales_plans_gsheet 
				where		No<>3 order by No
			) as top40
on 			lmm.Master_Account_ID = top40."Odoo ID"
-------
where	    lmm.Master_Account_Name <> 'migration traffic'
            --and ca.event_type is not NULL
group by	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
order by 	date(ca.hour_nk) desc