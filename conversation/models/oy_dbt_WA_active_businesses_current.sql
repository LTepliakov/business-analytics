{{ config(materialized = 'table') }}
{% set days_ago = 19 %}

{% for N in range(days_ago) %}
	(
	with active_clients_at as
	(
	select 			wab_id
					, conversation_direction
					, conversation_start::DATE as conversation_start
					, GETDATE()::DATE - {{ N }} - 1 as report_date
					, DATEDIFF(day, created_at::DATE, GETDATE()::DATE - {{ N }} - 1) as days_since_last_convers
	                , Master_Account_Name
	                , Master_Account_ID
	                , Username 
	                , Account_Domain 
	                , Account_Name
	from 			{{ ref('oy_dbt_conversation_wtsap_cost_and_conversations') }}
	where 			1=1
					and days_since_last_convers <= 30
					and days_since_last_convers >= 0
					and conversation_start::DATE>'2023-07-30'
	)
	select 			wab_id
					, conversation_direction
					, report_date
	                , Master_Account_Name
	                , Master_Account_ID
	                , Username 
	                , Account_Domain 
	                , Account_Name
					, max(conversation_start) as last_convers_at
					, min(days_since_last_convers) as days_since_last_convers
	from 			active_clients_at
	group by 		1,2,3,4,5,6,7,8
	)
	----------------
	{% if not loop.last %}
	UNION
	{% endif %}
{% endfor %}