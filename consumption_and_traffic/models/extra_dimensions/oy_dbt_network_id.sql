{{ config(materialized = 'ephemeral') }}

select      distinct country_id
            , operator_id
            , network_name
             , country_id||'/'||operator_id||'/'||network_name as cost_id
from        {{ source('aggregate', 'fact_sms_traffic_operator_aggregate')}}
where       1=1 
            and network_name is not null
            and country_id is not null
            and operator_id is not null