{{ config(materialized = 'ephemeral') }}

select 		* 
from 		{{ source('analytics','oy_dbt_sales_plans_gsheet')}} --sandbox.oy_sales_plans_gsheet 
where		No<>3 order by No