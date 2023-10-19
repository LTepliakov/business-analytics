select 		bundle_type 
			, current_selling_rate_value 
			--, cost_id
			, round(sum(units),0) as units
			, round(sum(charge),0) as charge
			, round(sum(Revenue_USD),0) as Revenue_USD 
from 		{{ source('analytics', 'oy_dbt_consumption_agg_sms_KSA')}}
where 		1=1
group by 	1,2
order by 	bundle_type, current_selling_rate_value desc