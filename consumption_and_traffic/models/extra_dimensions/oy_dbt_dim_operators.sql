with base as
(
select 		DISTINCT operator_id, operator_name  
from 		aggregate.fact_sms_traffic_operator_aggregate
where 		1=1
			and operator_name is not null
			and operator_id is not null
)
, cnt as 
(
select 		*
			, count(operator_name) over (partition by operator_name) operator_name_cnt
			, count(operator_id) over (partition by operator_id) operator_id_cnt
from 		base
)
select 		*
from 		cnt
where 		operator_name_cnt > 1
