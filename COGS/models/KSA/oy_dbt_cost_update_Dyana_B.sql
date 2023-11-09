with r as
(
select 			distinct
				country_id||'/'||operator_id||'/'||network_name as cost_id
				, country_id, operator_id, network_name , price, currency
				, case  when currency='USD' then price::NUMERIC
                        when currency='SAR' then price::NUMERIC/3.75
                        when currency='AED' then price::NUMERIC/3.67
                        end as cost_rate_USD
				, effective_date as effective_from
from 			{{ source('raw', 'mprocra__abl_operator_prices') }}
where 			type_of_route <> 'INTERNATIONAL'
order by 		cost_id 
)
, lead as 
(
select 			DENSE_RANK() over (order by cost_id) as drnk
				, count(cost_id) over (partition by cost_id) as cnt
				, *
				, lead(effective_from, 1) over (partition by cost_id order by effective_from) as effective_to
from 			r
)
select 			*
				, case when effective_to is null then current_date+1 else effective_to end as effective_to_var
from 			lead