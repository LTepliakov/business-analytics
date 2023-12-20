
select 		    Master_Account_ID 
                , Master_Account_Name 
                , date_nk 
                , count(*) as operator_traffic
                , sum(cost_rate_USD) as COGS_USD
                , sum(cost_rate_USD)/count(*) as avg_cost_rate_USD
from 		    {{ ref('oy_dbt_alaris_traffic_cost')}}
group by 	    1,2,3
--order by 	    Master_Account_ID 