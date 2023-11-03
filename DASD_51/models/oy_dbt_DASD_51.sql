
select          Main_accounts
                , MAX(consumption_last_30_days_flag) as consumption_last_30_days_flag
from            {{ ref('oy_dbt_merge')}}
group by        1