with base as
(
select 		DISTINCT network_id, network_name  
from 		aggregate.fact_sms_traffic_operator_aggregate
where 		1=1
			and network_name is not null
			and network_id is not null
)
, cnt as
(
select 		*
			, count(network_name) over (partition by network_name) network_name_cnt
			, count(network_id) over (partition by network_id) network_id_cnt
from 		base
)
select 		*
from 		cnt
--where 		network_id_cnt>1
--order by 	network_id 
