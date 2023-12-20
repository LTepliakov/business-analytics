{{ config(materialized='table')}}

with acc_match as
(
select 		distinct 
			ca.Master_Account_ID, cs.Master_Account_ID as M_Acc_ID
			, case when cs.Master_Account_ID is not null then 1 else 0 end as account_match
from 		{{ ref('oy_dbt_revenue_sms_International')}} as ca
left join 	{{ source('analytics', 'oy_dbt_avg_cost_rate_account_agg')}} as cs
on 			ca.Master_Account_ID = cs.Master_Account_ID 
)
, base as
(
select 		ca.*
			, am.account_match
			, cs.operator_traffic
			, cs.COGS_USD
			, cs.avg_cost_rate_USD
from 		{{ ref('oy_dbt_revenue_sms_International')}} as ca
left join 	{{ source('analytics', 'oy_dbt_avg_cost_rate_account_agg')}} as cs
on 			ca.date_nk = cs.date_nk 
			and ca.Master_Account_ID = cs.Master_Account_ID 
------------
left join 	acc_match as am
on 			ca.Master_Account_ID = am.Master_Account_ID
------------
where 		1=1
			and ca.date_nk >= '2023-11-17'
			and ca.Master_Account_Name <> 'Active monitoring'
order by 	ca.Master_Account_ID , ca.date_nk 
)
select 		rep_month
			, date_nk
			, Master_Account_ID
			, Master_Account_Name
			, account_match
			, chargeable_units
			, case when operator_traffic is null and account_match=1 then 0 else operator_traffic end as operator_traffic
			, case when operator_traffic is null and account_match=1 then chargeable_units else chargeable_units - operator_traffic end as units_delta
			, case when operator_traffic is null and account_match=1 then 100 else round(100*(chargeable_units/operator_traffic - 1), 1) end as units_delta_pct
			, Revenue_USD
			, case when operator_traffic is null and account_match=1 then 0 else COGS_USD end as COGS_USD
			, case when operator_traffic is null and account_match=1 then Revenue_USD else Revenue_USD - COGS_USD end as Gross_Profit
			, case when operator_traffic is null and account_match=1 then 100 else round(100*(Revenue_USD/COGS_USD - 1), 1) end as Gross_Margin
			, round(100*(avg_selling_rate_USD/avg_cost_rate_USD - 1), 1) as Theoretical_Gross_Margin
            , Revenue_USD*(avg_selling_rate_USD/avg_cost_rate_USD - 1) as  Theoretical_Gross_Profit
			, avg_selling_rate_USD
			, avg_cost_rate_USD
from 		base
where 		1=1 
			--and account_match = 1 and operator_traffic is null
order by 	Master_Account_ID , date_nk 