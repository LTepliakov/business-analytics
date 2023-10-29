{{ config(materialized='view')}}

-- I checked that there is no "cost_id  ilike '%internal%'" in this table
select 		rep_month 
			, date_nk 
			, charging_id 
			, Master_Account_ID 
			, Master_Account_Name 
			, cost_id 
			, sum(units) as consumption
			, sum(charge) as charge
			, sum(chargeable_units) as chargeable_units
			, sum(Revenue_USD) as Revenue_USD
			, sum(GP_USD_estimate) as GP_USD_estimate
			, MAX(categorized_charge_pct) AS categorized_charge_pct
			, MAX(charge_with_cost_id_pct) AS charge_with_cost_id_pct
from 		{{ ref('oy_dbt_revenue_sms_KSA')}}
where 		1=1
			--and traffic_is = 'KSA' - it's already all KSA in analytics.oy_dbt_consumption_agg_sms_KSA
group by 	1,2,3,4,5,6