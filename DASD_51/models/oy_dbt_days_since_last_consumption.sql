{{ config(materialized='ephemeral')}}

with base as
(
select      * 
from        {{ source('analytics', 'oy_dbt_consumption_agg')}} 
where       units <> 0
)
, ca as
(
select 		charging_id 
			, '2023-10-31'::DATE as report_date
			--, current_date as report_date
			, max(date_nk) as last_date_of_consumption
from 		base
where 		date_nk <= report_date
group by 	1,2
)
select 		charging_id
			, last_date_of_consumption
			, report_date
			, DATEDIFF(DAY, last_date_of_consumption, report_date) as days_since_last_consumption
from 		ca