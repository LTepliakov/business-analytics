{{ config(materialized='ephemeral')}}

with base as
(
select          a.*
                , d.last_date_of_consumption
                , d.report_date
                , d.days_since_last_consumption
from            {{ ref('oy_dbt_accounts_to_ckeck')}} as a
left join       {{ ref('oy_dbt_days_since_last_consumption')}} as d
on              a.Charging_ID_L0 = d.charging_id
)
select          *
                , case when days_since_last_consumption <= 30 then 1 else 0 end as consumption_last_30_days_flag
from            base
order by        Main_accounts