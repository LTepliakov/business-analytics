{{config (materialized='table')}} 

with base as
(
select          *
				, case when bundle_type = 'mb' then charge/units else null 
                       end as mb_sell_rate_USD
				, case when ocs_selling_rate >= 0.1 or ocs_selling_rate = 0
					   then null
					   else ocs_selling_rate
					   end as package_sell_rate_USD
from            {{ ref('oy_dbt_consumption_agg_sms')}} 
where 			1=1
				and units <> 0
)
, stg as
(
select 			*
				, case 	when bundle_type = 'mb' then mb_sell_rate_USD
						else package_sell_rate_USD
						end as current_selling_rate_value
from 			base
)
select          c.*
                , p.price as cost_rate
                , p.currency
                , p.cost_rate_USD
                , 100*(c.current_selling_rate_value/p.cost_rate_USD -1) as GM_pct_estimate
                , c.Revenue_USD*(c.current_selling_rate_value/p.cost_rate_USD -1) as GP_USD_estimate
from            stg as c
left join 	    {{ source('analytics','oy_dbt_cost_update_Dyana_B') }} as p
on 			    c.cost_id = p.cost_id
                and c.date_nk >= p.effective_from 
                and c.date_nk < p.effective_to_var
where           traffic_is in ('KSA', 'Unknown') -- Include unknow because of Alinma Bank use case which might contain failed campaigns traffic or can be a DQ issue