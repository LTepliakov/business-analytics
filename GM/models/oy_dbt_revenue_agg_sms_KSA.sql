{{ config(materialized='table')}}

select 		rep_month 
			, date_nk 
			, charging_id 
			, Master_Account_ID 
			, Master_Account_Name 
			, cost_id 
			, sum(units) as consumption
			, sum(charge) as charge
			, sum(Revenue_USD) as Revenue_USD
			, sum(GP_USD_estimate) as GP_USD_estimate
from 		{{ ref('oy_dbt_revenue_sms_KSA')}}
where 		traffic_is = 'KSA'
group by 	1,2,3,4,5,6