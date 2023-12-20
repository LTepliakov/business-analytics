select 		rep_month 
			, date_nk 
			, Master_Account_ID 
			, Master_Account_Name 
			, sum(chargeable_units) as chargeable_units
			, sum(Revenue_USD) as Revenue_USD
			, case when sum(chargeable_units) = 0 then null else sum(Revenue_USD)/sum(chargeable_units) end as avg_selling_rate_USD
from 		{{ ref('oy_dbt_consumption_agg_sms')}}
where 		1=1
			and traffic_is = 'International'
			--and date_nk >= '2023-11-17'
group by 	1,2,3,4