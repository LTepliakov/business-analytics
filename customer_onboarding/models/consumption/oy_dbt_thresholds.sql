{{ config(materialized = 'view')}}
with base as
(
select 		date_nk
			, split_part(event_type, '.',2) as channel
			, Master_Account_ID 
			, Master_Account_Name 
			, units
			, sum(units) over (partition by Master_Account_ID, event_type order by date_nk) as total_units
			, case  when event_type = 'default.sms' then 50000  --SMS (KSA)
					when event_type = 'default.flowstudio' then 500000 --Executions
					when event_type = 'default.chatbot' then 50000 --chatbot sessions
					when event_type = 'default.voice' then 10000 --Minutes
					when event_type = 'default.authenticate' then 90000 --Authentications
					end as threshold
			, case 	when event_type = 'default.sms' 
					then sum(units) over (partition by Master_Account_ID, event_type order by date_nk)
					else null
			  		end as sms_total_units
			, case 	when event_type = 'default.flowstudio' 
					then sum(units) over (partition by Master_Account_ID, event_type order by date_nk)
					else null
			  		end as flowstudio_total_units
			, case 	when event_type = 'default.chatbot' 
					then sum(units) over (partition by Master_Account_ID, event_type order by date_nk)
					else null
			  		end as chatbot_total_units
			, case 	when event_type = 'default.voice' 
					then sum(units) over (partition by Master_Account_ID, event_type order by date_nk)
					else null
			  		end as voice_total_units
			, case 	when event_type = 'default.authenticate' 
					then sum(units) over (partition by Master_Account_ID, event_type order by date_nk)
					else null
			 		end as authenticate_total_units
			, charging_id
			, pd_user_account_name 
from 		{{ source('analytics', 'oy_dbt_consumption_agg')}}
where 		1=1
			and event_type is not null
            and pd_user_account_name not in ('Active monitoring', 'Unifonic Employees')
			and Master_Account_Name not in ('Unifonic Employees')
            and Master_Account_ID is not null
)
select 		*
			, max(total_units) over (partition by Master_Account_ID, channel) as max_units_now
from 		base