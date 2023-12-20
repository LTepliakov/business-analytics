with base as
(
select 		    Master_Account_ID 
                , Master_Account_Name 
                , bundle_type 
                , round(sum(units),0) as units
                , round(sum(charge),0) as charge
                , round(sum(Revenue_USD),0) as Revenue_USD 
from		    {{ source('analytics', 'oy_dbt_consumption_agg_sms')}}
where 		    date_nk>='2023-08-01'
                and date_nk <= '2023-10-10' -- this filter is because from '2023-10-12' Alinma bank started to consume mb. I'll remove this filter later.
                and traffic_is = 'International'
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