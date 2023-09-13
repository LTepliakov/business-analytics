{{ config(materialized='ephemeral') }}

SELECT  	      date(fc.created_at) AS Creation_Date
                , lm.Master_Account_Name
                , lm.Master_Account_ID
                , fc.account_id
                , da.pd_user_account_name as Account_Name
                , da.pd_user_user_name as Username
                , split_part(da.pd_user_user_name,'@',2) as Account_Domain
                --, fc.end_customer_contact as End_User
                , CASE fc.channel	  WHEN 'whatsapp' THEN 'WHATSAPP'     
                                    WHEN 'messenger' THEN 'MESSENGER'     
                                    WHEN 'twitter' THEN 'TWITTER'     
                                    else fc.channel 
                  END as Channel
                , CASE fc.content_type WHEN 'TEMPLATE' THEN 'TEMPLATE' ELSE 'SESSION' END AS message_type
                , fc.comm_direction AS Direction
                , fc.product
                , nvl(COUNT(fc.record_id), 0) AS Message_Count
                , count(distinct fc.end_customer_contact) as End_User
from 		        {{ source('standard', 'fact_conversation') }} as fc 
----------------
left join 	    {{ source('standard', 'dim_accounts') }} as da 
on 			        fc.account_id =da.account_nk  
----------------
left join 	    {{ source('analytics', 'lm_v_master_account_mapping') }} as lm
on 			        fc.account_id =lm.UC_Account_ID_L0
----------------
where 		      year(fc.created_at) = 2023
                and month(created_at) > 6
----------------
group by  	    1,2,3,4,5,6,7,8,9,10,11