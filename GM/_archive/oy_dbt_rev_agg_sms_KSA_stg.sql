
with base as 
(
select 			*
				, case when bundle_type = 'mb' then charge/units else null end as mb_sell_rate_USD
				, case when current_selling_rate_value >= 0.1 or current_selling_rate_value = 0
					   then null
					   else current_selling_rate_value
					   end as package_sell_rate
from 			{{ source('analytics', 'oy_dbt_consumption_agg_sms_KSA')}}
where 			1=1
				and units <> 0
)
, stg as
(
select 			*
				, case 	when bundle_type = 'mb' then mb_sell_rate_USD
						else package_sell_rate
						end as selling_rate_stg
from 			base
)
select              rep_month
                ,	event_type
                ,	general_ledger_code
                ,	date_nk
                ,	charging_id
                ,	pd_user_id
                ,	pd_user_account_name
                ,	bundle_name
                ,	bundle_type
                ,	pd_user_country
                ,	manual_odoo_id
                ,	manual_odoo_name
                ,	tariff
                ,	Master_Account_ID
                ,	Master_Account_Name
                ,	Master_Account_Origin
                ,	hs_team
                ,	hs_manager
                ,	tcsm_manager
                ,	Account_Type
                ,	client_rank
                ,	country_id
                ,	operator_id
                ,	network_name
                ,	cost_id
                --,	current_selling_rate_value
                ,	selling_rate_stg as current_selling_rate_value
                ,	units
                ,	charge
                ,	Revenue_USD
                ,	network_is
                ,	traffic_is
                ,	cost_id_is
                ,	client_charge
                ,	categorized_charge
                ,	charge_with_cost_id
                ,	categorized_charge_agg
                ,	charge_with_cost_id_agg
                ,	categorized_charge_pct
                ,	charge_with_cost_id_pct
                ,	cost_rate
                ,	currency
                ,	cost_rate_USD
                ,	GM_pct_estimate
                ,	GP_USD_estimate
                --,	mb_sell_rate_USD
                --,	package_sell_rate
                --,	selling_rate_stg
from            stg