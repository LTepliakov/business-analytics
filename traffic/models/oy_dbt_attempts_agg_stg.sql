{{ config(materialized='table') }}

select * from analytics.oy_dbt_attempts_agg_stg_2023
union 
select * from {{ ref('oy_dbt_attempts_agg_stg_current')}}