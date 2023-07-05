{{ config(materialized='table') }}

select 		RANK () over (partition by id order by max_ingestion_date) as rnk
			, COUNT(id) over (partition by id) as cnt
			, a.*
			, da.pd_user_account_name, da.pd_user_user_name
			, lm.Master_Account_Name, lm.Master_Account_ID
from 		(
				select 		charging_id, id, name, selling_rate_numeric
							, max(ingestion_date) as max_ingestion_date 
				from 		(
								select 		a.account_id as charging_id, b.*
											, left(split_part(selling_rate,'USD',1), 8)::numeric(20,6) AS selling_rate_numeric  
								from 		{{ source('raw', 'ocs__balance')}} as b --raw.ocs__balance
								left join	{{ source('raw', 'ocs__account')}} as a --raw.ocs__account
								on 			b.account_id = a.id
								where 		balance_type=2
											and SELLING_RATE not ilike '%SAR%'
											--and ingestion_date<='2023-04-26' 
											--the date range is chosen based on the bugfix in this Slack thread:
											--https://unifonic.slack.com/archives/C04KV5EGUDC/p1682572150715099?thread_ts=1682441533.662779&cid=C04KV5EGUDC
											--and a.account_id = 10005213
							)x
				group by 	1,2,3,4
			) as a
---
left join 	{{ source('standard', 'dim_accounts') }} as da --standard.dim_accounts 
on 				a.charging_id = da.charging_id::VARCHAR2 
---
left join 	{{ source('analytics', 'lm_v_master_account_mapping')}} as lm --analytics.lm_v_master_account_mapping
on 			a.charging_id = lm.Charging_ID_L0::VARCHAR2 
---
where  		1=1
			and selling_rate_numeric<>1 and selling_rate_numeric<>0 and selling_rate_numeric is not null 
order by 	id , max_ingestion_date