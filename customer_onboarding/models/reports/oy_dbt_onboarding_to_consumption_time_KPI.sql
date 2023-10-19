select              *
from                {{ ref('oy_dbt_onboarding_to_consumption_time')}} as c
where               days_to_threshold_hit>0 
                    or days_to_last_consumption>0
