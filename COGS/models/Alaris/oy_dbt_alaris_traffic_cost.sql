{{ config(materialized='table')}}

select          * 
from            {{ ref('oy_dbt_alaris_data_modified')}}
where           1=1
                and is_successful = 1
                and date_nk >= '2023-11-17'