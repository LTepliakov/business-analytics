{{ config(materialized = 'ephemeral')}}

with num as
(
select 		ROW_NUMBER() over (partition by Master_Account_ID, channel order by date_nk) as rn
			, *
from 		{{ ref('oy_dbt_thresholds')}}
where 		1=1
			and		(	
					sms_total_units >= threshold --SMS (KSA)
					or flowstudio_total_units >= threshold --Executions
					or chatbot_total_units >= threshold --chatbot sessions
					or voice_total_units >= threshold --Minutes
					or authenticate_total_units >= threshold --Authentications
				   	)
)
select 		* 
from 		num 
where 		rn = 1
order by 	Master_Account_Name , channel