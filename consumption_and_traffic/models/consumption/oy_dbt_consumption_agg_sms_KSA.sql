{{config (materialized='view')}} 

select          c.*
                , p.cost_rate_USD
                , 100*(c.current_selling_rate_value/p.cost_rate_USD -1) as GM_pct_estimate
                , c.Revenue_USD*(c.current_selling_rate_value/p.cost_rate_USD -1) as GP_USD_estimate
from            {{ ref('oy_dbt_consumption_agg_sms')}} as c
left join 	    {{ source('analytics','oy_dbt_cost_update_Dyana_B') }} as p
on 			    c.cost_id = p.cost_id
                and c.date_nk >= p.effective_from 
                and c.date_nk < p.effective_to_var
where           traffic_is = 'KSA'