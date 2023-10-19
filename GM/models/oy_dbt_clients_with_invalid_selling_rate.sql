
select 		Master_Account_ID 
			, Master_Account_Name 
			, current_selling_rate_value
			, round(sum(units),0) as units
			, round(sum(charge),0) as charge
			, round(sum(Revenue_USD),0) as Revenue_USD 
from 		{{ source('analytics', 'oy_dbt_consumption_agg_sms_KSA')}}
where 		1=1 
			and bundle_type <> 'mb'
			and date_nk>='2023-08-01'
			and (
				current_selling_rate_value = 1
				or current_selling_rate_value = 0
				or current_selling_rate_value is null
				)
group by 	1,2,3
order by 	Revenue_USD  desc