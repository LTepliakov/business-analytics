{{ config(materialized='table') }}

with da as 
(
select  	    account_nk as Account_ID
                , pd_user_user_name as Username
                , split_part(pd_user_user_name,'@',2) as Account_Domain
                , pd_user_account_name as Account_Name  
from 		    {{ source('standard', 'dim_accounts') }}
)
select 			wc.* 
                , lm.Master_Account_Name
                , lm.Master_Account_ID
                , da.Username 
                , da.Account_Domain 
                , da.Account_Name
from 			{{ source('raw', 'conversation__whatsapp_conversation_based_charge') }} as wc
----------------
left join		da
on 				wc.account_id = da.Account_ID
----------------
left join 	    {{ source('analytics', 'lm_v_master_account_mapping') }} as lm
on 			    wc.account_id = lm.UC_Account_ID_L0 
