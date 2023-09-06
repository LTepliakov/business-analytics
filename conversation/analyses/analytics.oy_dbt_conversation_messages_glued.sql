select * from "verticadb"."standard"."fact_conversation" order by created_at desc
;
drop table if exists analytics.oy_conversation_messages_2022_Q3
;
create table analytics.oy_conversation_messages_2022_Q3 as 
SELECT  	    date(fc.created_at) AS Creation_Date
                , lm.Master_Account_Name
                , lm.Master_Account_ID
                , fc.account_id
                , da.pd_user_account_name as Account_Name
                , da.pd_user_user_name as Username
                , split_part(da.pd_user_user_name,'@',2) as Account_Domain
                , fc.end_customer_contact as End_User
                , CASE fc.channel	WHEN 'whatsapp' THEN 'WHATSAPP'     
                                    WHEN 'messenger' THEN 'MESSENGER'     
                                    WHEN 'twitter' THEN 'TWITTER'     
                                    else fc.channel 
                  END as Channel
                , CASE fc.content_type WHEN 'TEMPLATE' THEN 'TEMPLATE' ELSE 'SESSION' END AS message_type
                , fc.comm_direction AS Direction
                , fc.campaign_id
                , fc.product
                , nvl(COUNT(fc.record_id), 0) AS Message_Count  
                , count(distinct conversation_id) as Conversation_Count
from 		    "verticadb"."standard"."fact_conversation" as fc 
----------------
left join 	    "verticadb"."standard"."dim_accounts" as da 
on 			    fc.account_id =da.account_nk  
----------------
left join 	    "verticadb"."analytics"."lm_dbt_master_account_mapping" as lm
on 			    fc.account_id =lm.UC_Account_ID_L0 
----------------
where 		    year(fc.created_at) = 2023
				and month(created_at) > 6 
				and month(created_at) < 10
group by  	    1,2,3,4,5,6,7,8,9,10,11,12,13
;
drop table if exists analytics.oy_dbt_conversation_messages
;
create table analytics.oy_dbt_conversation_messages as
select * from analytics.oy_conversation_messages_2023_Q1 --order by Creation_Date desc
union
select * from analytics.oy_conversation_messages_2023_Q2 --order by Creation_Date desc
union
select * from analytics.oy_conversation_messages_2022_Q3 --order by Creation_Date desc
--union
--select * from analytics.oy_conversation_messages_2022_Q4 --order by Creation_Date --desc
;
select * from analytics.oy_dbt_conversation_messages order by Creation_Date desc






