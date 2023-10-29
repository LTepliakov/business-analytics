{{ config(materialized='table')}}

select 			c.rep_month 
				, c.date_nk 
				, c.charging_id 
				, c.Master_Account_ID 
				, c.Master_Account_Name 
				, c.cost_id 
				, c.consumption 
				, c.charge
				, c.chargeable_units
				, t.units as traffic
				, c.Revenue_USD 
				, t.COGS_USD
				, case when t.COGS_USD is null then c.Revenue_USD
					   else c.Revenue_USD - t.COGS_USD 
					   end as Gross_Profit_USD
				, c.GP_USD_estimate
				, case when t.COGS_USD is null or t.COGS_USD = 0 then null 
					   else round(100*(c.Revenue_USD/t.COGS_USD - 1), 2) 
					   end as Gross_Margin_pct
				, case when c.Revenue_USD is null or c.Revenue_USD = 0 then null
					   else round(100*(c.GP_USD_estimate/c.Revenue_USD), 2) 
					   end as GM_pct_estimate
				, t.cost_rate_USD 
				, t.cost_rate
				, t.currency
				, t.network_name
				, t.operator_name
				, c.categorized_charge_pct
				, c.charge_with_cost_id_pct
from 			{{ ref('oy_dbt_revenue_sms_KSA_agg')}} as c
left join 		{{ ref('oy_dbt_KSA_COGS_excl_failover_agg')}} as t
on				c.date_nk = t.date_nk 
				and c.cost_id = t.cost_id 
				and c.charging_id = t.charging_id 
where 			1=1
--order by 		charging_id , cost_id 