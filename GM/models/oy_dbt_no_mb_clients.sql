with base as
(
select 		    Master_Account_ID 
                , Master_Account_Name 
                , bundle_type 
                , round(sum(units),0) as units
                , round(sum(charge),0) as charge
                , round(sum(Revenue_USD),0) as Revenue_USD 
from		    {{ source('analytics', 'oy_dbt_consumption_agg_sms_KSA')}}
where 		    date_nk>='2023-08-01'
group by 	    1,2,3
order by 	    Master_Account_ID 
)
, pmix as
(
select 		    *
			    , count(Master_Account_ID) over (partition by Master_Account_ID ) as cnt
from 		    base
)
select 		    *
from 		    pmix
where 		    cnt = 1 
                and bundle_type = 'package'