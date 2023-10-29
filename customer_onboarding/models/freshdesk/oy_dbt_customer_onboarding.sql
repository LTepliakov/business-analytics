{{ config(materialized='view')}}

with l as
(
select 		id as fs_ticket_id 
			, company_id as fs_company_id
			, stats__closed_at 
			, datediff(day, created_at, stats__closed_at) as days_to_close
			, created_at , stats__first_responded_at , stats__agent_responded_at
			, datediff(day, created_at, stats__first_responded_at) as days_first_responce
			, datediff(day, created_at, stats__agent_responded_at) as days_agent_responce
			--, due_by , fr_due_by , status
			, group_id , requester_id , requester_name 
			, responder_id 
			--, "source" 
			--, custom_fields__cf_req_category 
			, custom_fields__cf_req_template 
			, custom_fields__cf_service781181 
			, custom_fields__cf_company_name
			, requester_email 
			--, subject, description_text 
from 		{{ source('raw','freshdesk__tickets')}}
where 		type = 'Onboarding Request'
			--and company_id is not null
)
select 		companies.custom__fields_odoo_id as fs_odoo_id
			, companies.name as fs_company_name
			, companies.custom__fields_hubspot_company_name as hubspot_company_name
			, companies.custom__fields_hubspot_id as hubspot_id
			, companies.custom__fields_uc_id as fs_uc_id
			, l.*
			, companies.industry as industry_name
			, agents.contact_name
from		l
left join	{{ source('raw','freshdesk__agents')}} as agents
on 			l.responder_id = agents.id
------------
left join	{{ source('raw','freshdesk__groups')}} as groups
on 			l.group_id = groups.id
------------
left join	{{ source('raw','freshdesk__companies')}} as companies
on 			l.fs_company_id = companies.id
------------
left join	{{ source('analytics', 'lm_v_master_account_mapping')}} as lm
on 			companies.custom__fields_uc_id::VARCHAR = lm.UC_Account_ID_L0