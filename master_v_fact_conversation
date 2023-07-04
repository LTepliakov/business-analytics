drop view if exists master_v_fact_conversation
;
create view master_v_fact_conversation as
select		*
from		(
				SELECT  	date( fc.created_at) AS Creation_Date
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
							, nvl(COUNT(fc.record_id), 0) AS Message_Count  
				from 		standard.fact_conversation as fc  
				join 		standard.dim_accounts as da 
				on 			fc.account_id =da.account_nk  
				where 		fc.created_at>= '2022-01-01 00:00:00'   
				group by  	1,2,3,4,5,6,7,8,9
			) as fc
left join 	(
				select 		account_id as fb_account_id
							, sum(conversation_count) as conversation_count , sum(conversation_cost) as conversation_cost
				from 		raw.conversation__whatsapp_conversation_based_charge
				group by 	1
			) as wa
on 			fc.account_id = wa.fb_account_id
;