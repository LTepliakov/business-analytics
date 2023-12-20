{{config (materialized='table')}}

select 			date_hour::DATE as date_nk
				, charging_id
				, network_id
				, network_name
				, operator_id
				, operator_name
				, country_id
				, destination_country
				, message_status
				, event_status
				--, failure
				, mnp_used
				--, normalized_reason
				--, normalized_status
				, account_country
				, sum(units) as units
from 			{{ source('aggregate', 'fact_sms_traffic_operator_aggregate') }} as t 
----------------
where		    1=1
group by 	    1,2,3,4,5,6,7,8,9,10,11,12