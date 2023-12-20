{{ config(materialized='table') }}

with base as
(
select         	hour_nk::DATE as date_nk
				, account_id as charging_id
				, bundle_name
				, action
				, result
				, host_name
				, event_type
				, event_source
				, number_portability_routing_information
				, country
				, sender_name
				--, revenue
				, operator_id
				, network_name
				, country_id
				, tariff
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
				, current_selling_rate_value
				, current_selling_rate_currency
				, exchange_rate
				, sum(units) as units
				, sum(charge) as charge
				, sum(message_count) as message_count
				, sum(chargeable_units) as chargeable_units
from           	{{ source('analytics', 'oy_dbt_fact_sms_consumption_aggregate_stg')}}
where 			1=1
                and hour_nk >= '2023-12-01 00:00:00.000' -- selling rates are correct from 30-Nov-2023 see https://unifonic.slack.com/archives/C03QRQ12M71/p1701324586677169
                and (event_type = 'default.sms' or general_ledger_code = 'Forfeiture Of Package Balance')
				and units > 0
				and Master_Account_Name <> 'UC.Unifonic.Test'
				and result = 'SUCCESS'
				and action = 'DIRECT_DEBIT'
group by 		1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35
)
select          *
                , case  when bundle_type = 'mb' then charge / chargeable_units
                        else current_selling_rate_value
                        end as selling_rate
				, case 	when bundle_type = 'package' then chargeable_units * current_selling_rate_value
						else charge 
						end as Revenue
from            base