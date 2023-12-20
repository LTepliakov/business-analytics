{{ config(materialized='table') }}

with base as
(
select * from analytics.oy_dbt_consumption_daily_2022
union 
select * from analytics.oy_dbt_consumption_daily_2023
union
select * from {{ ref('oy_dbt_consumption_daily_current')}}
)
select          *
from            base
where 			1=1
				and Master_Account_Name <> 'UC.Unifonic.Test'
				and result = 'SUCCESS'
				and action = 'DIRECT_DEBIT'