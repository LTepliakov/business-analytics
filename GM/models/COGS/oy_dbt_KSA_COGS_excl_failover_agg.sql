{{ config(materialized='table')}}

select 		date_nk
			, charging_id 
			, Master_Account_ID 
			, Master_Account_Name 
			, destination_country 
			, operator_name 
			, network_name 
			, country_id 
			, operator_id 
			, cost_id 
			, cost_rate 
			, currency 
			, cost_rate_USD 
			, sum(units) as units
			, sum(COGS) as COGS
			, sum(COGS_USD) as COGS_USD 
from 		{{ source('analytics', 'oy_dbt_KSA_COGS_excl_failover')}}
where 		1=1
			and date_nk >= '2023-08-01' 
group by 	1,2,3,4,5,6,7,8,9,10,11,12,13