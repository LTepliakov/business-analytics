{{ config(materialized='table')}}

select      Master_Account_ID
            , Master_Account_Name
            , channels_count
            , channel
            --, date_nk -- if pct_to_threshold = 1 then date_nk is the date when a threshold got hit else it's a last consumption date
            , case when pct_to_threshold=100 then date_nk else null end as threshold_hit_at
            , case when pct_to_threshold<100 then date_nk else null end as last_consumption_at
            , pct_to_threshold
            , total_units
            , threshold
from        {{ ref('oy_dbt_consumption_start_date')}}