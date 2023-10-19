with base as
(
select 		distinct fs_company_id, stats__closed_at
from 		{{ ref('oy_dbt_customer_onboarding')}}
)
, stg1 as
(
select 		ROW_NUMBER() over (partition by fs_company_id order by stats__closed_at) as rn 
			, count(fs_company_id) over (partition by fs_company_id) as cnt
			, *
from 		base
)
, stg2 as
(
select 		*
			, case 	when cnt = 1 and stats__closed_at is null then 'no'
					else 'yes'
			   end as at_least_one_ticket_closed
from 		stg1
--order by 	fs_company_id , rn
)
select 		cnt as count_onboarding_tickets
			, fs_company_id 
			, stats__closed_at::DATE as first_closing_date
			, at_least_one_ticket_closed
from 		stg2
where 		rn = 1
--order by 	fs_company_id , rn