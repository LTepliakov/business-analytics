{{ config(materialized='view')}}

with base as
(
select 		DISTINCT country_id , destination_country 
from 		aggregate.fact_sms_traffic_operator_aggregate
where 		1=1
			and destination_country is not null 
			and country_id is not null
			--country_id in ('827','753')
)
select 		*
			, count(destination_country) over (partition by destination_country) destination_country_cnt
from 		base