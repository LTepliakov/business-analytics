create view analytics.oy_v_customer_onboarding as 
select 		agents.contact_name, agents.type , agents.contact__time_zone
			, groups.name as group_name
			, companies.name as company_name
			, companies.industry as industry_name
			, l.* 
from		(
				select 		created_at , stats__first_responded_at , stats__agent_responded_at , stats__closed_at
							, datediff(day, created_at, stats__first_responded_at) as days_first_responce
							, datediff(day, created_at, stats__agent_responded_at) as days_agent_responce
							, datediff(day, created_at, stats__closed_at) as days_to_close
							, due_by , fr_due_by , status
							, id , group_id , company_id , requester_id , requester_name 
							, responder_id , "source" , custom_fields__cf_req_category 
							, custom_fields__cf_req_template , custom_fields__cf_service781181 
							, custom_fields__cf_company_name
							, description_text , requester_email , subject 
				from 		raw.freshdesk__tickets
				where 		type = 'Onboarding Request'
			) as l
left join	raw.freshdesk__agents as agents
on 			l.responder_id = agents.id
left join	raw.freshdesk__groups as groups
on 			l.group_id = groups.id
left join	raw.freshdesk__companies as companies
on 			l.company_id = companies.id