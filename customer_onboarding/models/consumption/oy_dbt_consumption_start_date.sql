
with base as
(
select 		*, 100 as pct_to_threshold 
from 		{{ ref('oy_dbt_threshold_reached')}}
union
select 		*, round(100*total_units/threshold,1) as pct_to_threshold 
from 		{{ ref('oy_dbt_threshold_not_reached')}}
)
select 		count(Master_Account_ID) over (partition by Master_Account_ID) as channels_count
			, *
from 		base