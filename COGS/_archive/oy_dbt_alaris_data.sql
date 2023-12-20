{{ config(materialized='table')}}

with src as 
(
select 		    * 
                , case  when regexp_like(src_service_type, '^(0[xX])?[0-9a-fA-F]+$') 
                        then src_service_type
                        else null
                        end as pd_user_id_hex 
                , case  when v_currency = 'UEtS' then 'PKR'
                        when v_currency = 'RUdQ' then 'EGP'
                        when v_currency = 'RVVS' then 'EUR'
                        when v_currency = 'QUVE' then 'AED'
                        when v_currency = 'VVNE' then 'USD'
                        else null
                        end as currency
                , case  when v_currency = 'UEtS' then 277.69 --'PKR'
                        when v_currency = 'RUdQ' then 30.9 --'EGP'
                        when v_currency = 'RVVS' then 0.94 --'EUR'
                        when v_currency = 'QUVE' then 3.67 --'AED'
                        when v_currency = 'VVNE' then 1 --'USD'
                        else null
                        end as fx_rate
                , case  when sar_parts = 0 and is_successful = 1 then 1 else sar_parts end as operator_traffic
from		    {{ source('standard', 'fact_acdr')}}
where           1=1
                and is_successful = 1 
                and v_rate_value <> 0
                --and sar_parts <> 0 --for all incoming_protocol = 'HTTP' sar_parts = 0
)
, base as
(
select 		    date_nk
                , local_id
                , src_service_type
                , hex_to_integer(pd_user_id_hex) as pd_user_id
                , ani as sender_id
                , mccmnc
                , country
                , incoming_protocol
                , c_channel_id as connection_type
                , edr_dial_code as country_code
                , dst_channel_id
                , client_operator_name
                , vendoroperator_name as vendor_name
                , vendor_prod_id
                , client_prod_id
                , status
                , dlr_status
                , sar_parts 
                , operator_traffic
                , v_rate_value as cost_rate
                , currency||' '||v_rate_value as cost_rate_cat
                , v_rate_value * operator_traffic as COGS
                , currency
                , fx_rate
                , v_rate_value / fx_rate as cost_rate_USD
                , v_rate_value * operator_traffic / fx_rate as COGS_USD
from 		    src
)
select 		    lm.Master_Account_ID
                , lm.Master_Account_Name
                , lm.Charging_ID_L0
                , b.*		
from 		    base as b
left join 	    {{ source('analytics', 'lm_v_master_account_mapping')}} as lm
on			    b.pd_user_id = lm.PD_User_ID_L0