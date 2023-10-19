
{{ config(materialized='table')}}

with base as
(
select 			--ROW_NUMBER() over (partition by fs_company_id order by stats__closed_at) as rn 
				ROW_NUMBER() over (partition by fs_company_id, custom_fields__cf_service781181 order by created_at) as rn 
				, *
from 			{{ ref('oy_dbt_customer_onboarding')}}
)
, dd as
(
select 			* 
from 			base
where 			rn = 1
)
select 			cc.count_onboarding_tickets
				, cc.first_closing_date
				, cc.at_least_one_ticket_closed
				, dd.*
				, case 	when dd.custom_fields__cf_service781181='SMS' then 'sms'
						when dd.custom_fields__cf_service781181='WAB' then 'chatbot'
						when dd.custom_fields__cf_service781181='Voice' then 'voice'
						when dd.custom_fields__cf_service781181='flowstudio' then 'flowstudio'
						else dd.custom_fields__cf_service781181
						end as channel
from 			dd
left join 		{{ ref('oy_dbt_onboarding_tickets_closing_summary')}} as cc
on 				dd.fs_company_id = cc.fs_company_id