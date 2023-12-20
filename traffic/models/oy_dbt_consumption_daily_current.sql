select 			date_nk
				--, hour_nk
				, account_id as charging_id
				, bundle_name
				, action
				, result
				, host_name
				, event_type
				, event_source
				--, number_portability_routing_information
				, country
				, sender_name
				, operator_id
				, network_name
				, country_id
				, tariff
				, current_selling_rate_value
				, current_selling_rate_currency
				, exchange_rate
				, general_ledger_code
				, line_item_id
				, bundle_start_date
				, bundle_expiry_date
				, bundle_type
				, pd_user_id
				, pd_user_account_name
				, pd_user_country
				, Master_Customer_ID
				, Master_Account_ID
				, Master_Account_Name
				, Master_Account_Origin
				, hs_team
				, hs_manager
				, tcsm_manager
				, Account_Type
				, client_rank 
				, sum(units) as units
				, sum(charge) as charge
				, sum(chargeable_units) as chargeable_units
				, sum(message_count) as message_count
				--, revenue
from 			{{ source('analytics','oy_dbt_fact_sms_consumption_aggregate_stg')}}
where 			date_nk >= '2023-12-01'
group by 		1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34