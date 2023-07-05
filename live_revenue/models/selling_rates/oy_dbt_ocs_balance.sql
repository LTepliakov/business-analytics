{{ config(materialized='table') }}

select 	*
		, case 	when 	split_part(selling_rate,'USD',1)::numeric(10,8) > 0 and split_part(selling_rate,'USD',1)::numeric(10,8)<>1 
						then left(split_part(selling_rate,'USD',1), 10)::numeric(10,8) 
				else 	null
		  end as raw_selling_rate
from 	{{ source('raw', 'ocs__balance')}} --raw.ocs__balance 
where 	1=1 
		and SELLING_RATE not ilike '%SAR%'
		and balance_type=2
-----
union
select 	*
		, case 	when 	split_part(selling_rate,'SAR',1)::numeric(10,8) > 0 and split_part(selling_rate,'SAR',1)::numeric(10,8)<>1
						then left(split_part(selling_rate,'SAR',1), 10)::numeric(10,8) 
				else 	null 
		  end as raw_selling_rate
from 	{{ source('raw', 'ocs__balance')}} --raw.ocs__balance
where 	1=1 
		and SELLING_RATE ilike '%SAR%'
		and balance_type=2
-----
union
select 	*
		, SELLING_RATE::numeric(10,8) as raw_selling_rate
from 	{{ source('raw', 'ocs__balance')}} --raw.ocs__balance 
where 	1=1 
		and SELLING_RATE is null	
		and balance_type=2
