{{ config(materialized='table')}}

with sa as
(
select 			    submit_date 
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
                    , account_id
                    , master_account_id as charging_id
                    , sum(message_count) as message_count 
                    , sum(number_of_units) as number_of_units 
from 			    aggregate.fact_sms_aggregate
--where 			submit_date >= '2023-10-10'
group by 		    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
,
base as
(
select			    sa.*
                    , da.pd_user_id
                    , COALESCE (lm.Master_Account_ID , lm1.Master_Account_ID ) as Master_Account_ID
                    , COALESCE (lm.Master_Account_Name , lm1.Master_Account_Name ) as Master_Account_Name
                    , COALESCE (lm.Main_Sub_Flag , lm1.Main_Sub_Flag ) as Main_Sub_Flag
from 			    sa
----------------
left join 		    standard.dim_fcdr_accounts as da
on 				    sa.account_id = da.fcdr_account_nk 
----------------
left join 		    analytics.lm_dbt_master_account_mapping as lm
on 				    da.account_nk = lm.UC_Account_ID_L0 
----------------
left join 		    analytics.lm_dbt_master_account_mapping as lm1
on 				    sa.charging_id = lm.Charging_ID_L0 
)
select 			    b.*
				    , rp.uc_customer_id as customer_id
from 			    base as b
left join 		    raw.odoo__res_partner as rp
on 				    b.Master_Account_ID = rp.id 