{{ config(materialized='table')}}

select 			c.rep_month 
				, c.date_nk 
				, c.charging_id 
				, c.Master_Account_ID 
				, c.Master_Account_Name 
				, c.cost_id 
				, c.consumption 
				, c.charge
				, t.units as traffic
				, c.Revenue_USD 
				, t.COGS_USD
				, c.Revenue_USD - t.COGS_USD as Gross_Profit_USD
				, c.GP_USD_estimate
				, round(100*(c.Revenue_USD/t.COGS_USD - 1), 2) as Gross_Margin_pct
				, round(100*(c.GP_USD_estimate/c.Revenue_USD), 2) as GM_pct_estimate
				, t.cost_rate_USD 
from 			{{ ref('oy_dbt_revenue_agg_sms_KSA')}} as c
full join 		{{ source('analytics', 'oy_dbt_KSA_COGS_excl_failover')}} as t
on				c.date_nk = t.date_nk 
				and c.cost_id = t.cost_id 
				and c.charging_id = t.charging_id 
where 			1=1
				and c.date_nk > '2023-08-01' 
				and t.date_nk > '2023-08-01'
--order by 		charging_id , cost_id 