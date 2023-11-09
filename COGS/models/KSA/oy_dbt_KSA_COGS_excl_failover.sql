{{config (materialized='view')}}

select 		    t.*
                , p.price as cost_rate
                , p.currency
                , p.cost_rate_USD
                , t.units*p.price as COGS
                , t.units*p.cost_rate_USD as COGS_USD
from 		    {{ source('analytics', 'oy_dbt_traffic_operator_agg_KSA') }} as t 
----------------
left join 	    {{ ref('oy_dbt_cost_update_Dyana_B') }} as p
on 			    t.cost_id = p.cost_id 
                and t.date_nk >= p.effective_from 
                and t.date_nk < p.effective_to_var
----------------
where		    1=1
                and t.date_nk >= '2023-08-01'
                and p.price is not null -- exluding failovers