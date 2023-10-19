{{ config(materialized = 'ephemeral')}}

with num as
(
select 		ROW_NUMBER() over (partition by Master_Account_ID, channel order by date_nk desc) as rn
			, *
from 		{{ ref('oy_dbt_thresholds')}}
where 		1=1
			and max_units_now < threshold
)
select 		* 
from 		num 
where 		rn = 1
order by 	Master_Account_Name , channel