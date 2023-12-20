{{config (materialized='table')}} 

select 	        LAST_DAY(ADD_MONTHS(hour_nk, -1)) + 1 as rep_month
                , event_type
                , general_ledger_code
                , date_nk
                , account_id as charging_id
                , pd_user_id
                , pd_user_account_name
                , bundle_name
                , bundle_type
                , pd_user_country
                , case 	when bundle_name in ('10002202_UAE All Operators', '10002202_mb') then '9598' -- Tamara
                        when bundle_name in ('20000600_UAE All Operators') then '10168' -- Tabby
                        when account_id = 114 then '3436'
                        else null 
                        end as manual_odoo_id
                , case 	when bundle_name in ('10002202_UAE All Operators', '10002202_mb') then 'Tamara FZE' -- Tamara
                        when bundle_name in ('20000600_UAE All Operators') then 'Tabby FZ-LLC' -- Tabby
                        else null 
                        end as manual_odoo_name
                , tariff
                , Master_Account_ID
                , Master_Account_Name
                , Master_Account_Origin
                , hs_team
                , hs_manager
                , tcsm_manager
                , Account_Type
                , client_rank
                , country_id
                , operator_id
                , network_name
                , case  when country_id is not null and operator_id is not null and network_name is not null
                        then country_id||'/'||operator_id||'/'||network_name
                        else null
                        end as cost_id
                , current_selling_rate_value as ocs_selling_rate
                , sum(units) as units
                , sum(charge) as charge
                , sum(chargeable_units) as chargeable_units
                , sum(case  when split_part(bundle_name,'_',2)='mb' then charge
                        else charge*current_selling_rate_value
                        end
                ) as Revenue_USD
from 		{{ ref('oy_dbt_fact_sms_consumption_aggregate_stg')}}
group by	1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26