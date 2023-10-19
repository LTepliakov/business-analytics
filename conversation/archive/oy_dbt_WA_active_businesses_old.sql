{{ config(materialized = 'table') }}
{% set days_ago = 31 %}

{% for N in range(days_ago) %}
	(
	with active_clients as
	(
	select 			account_id
					, created_at::DATE 
					, time_ingested ::DATE
					, GETDATE()::DATE - {{ N }} - 1 as report_date
					, DATEDIFF(day, created_at::DATE, GETDATE()::DATE - {{ N }} - 1) as days_since_last_message
	from 			{{ source('standard', 'fact_conversation') }}
	where 			1=1
					and days_since_last_message <= 30
					and days_since_last_message >= 0
					and comm_direction = 'OUTBOUND'
	)
	select 			ac.account_id
					, da.pd_user_account_name
					, da.pd_user_user_name
					, lm.Master_Account_Name
					, lm.Master_Account_ID
					, ac.report_date
					, max(created_at) as last_created_at
					, max(time_ingested) as time_ingested
					, min(days_since_last_message) as days_since_last_message
	from 			active_clients as ac
	----------------
	left join 		{{ source('standard','dim_accounts') }} as da
	on 				ac.account_id = da.account_nk
	----------------
	left join 		 {{ source('analytics', 'lm_v_master_account_mapping') }} as lm
	on 				ac.account_id =lm.UC_Account_ID_L0
	----------------
	group by 		1,2,3,4,5,6
	)
	----------------
	{% if not loop.last %}
	UNION
	{% endif %}
{% endfor %}