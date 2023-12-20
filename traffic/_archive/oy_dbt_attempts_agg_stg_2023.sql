select 			submit_date
				, account_id
				, source_protocol
				, message_status
				, sender_name
				, operator_id
				, country_id
				, network_id
				, region
				, message_type
				, event_status
				, mnp_used
				, normalized_status
				, dlr_status
				, campaign_id
				, product
				, product_name
				--, master_account_id, adjusted_cost_units, correlation_id, fc_account_id, fc_bundle_name, fc_event_type, fc_action, fc_pd_user_id, fc_units, Sum_Of_Charge, Sum_Of_Cost
				, cdr_kind
				, customer_status
				, customer_reason
				, sum(message_count) as message_count
				, sum(number_of_units) as number_of_units
from 			{{ source('aggregate', 'fact_sms_aggregate_daily_view')}}
where 			submit_date>='2023-01-01' and submit_date<='2023-11-30'
group by 		1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20