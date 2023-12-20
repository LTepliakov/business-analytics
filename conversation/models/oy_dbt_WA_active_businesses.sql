{{ config(materialized = 'table') }}

select * from {{ ref('oy_dbt_WA_active_businesses_current')}}
union
select * from analytics.oy_dbt_WA_active_businesses_Sep
union
select * from analytics.oy_dbt_WA_active_businesses_Oct
union
select * from analytics.oy_dbt_WA_active_businesses_Nov
