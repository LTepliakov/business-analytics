{{ config(materialized='view')}}

with base as
(
select          a.*
                , d.report_date
                , d.last_date_of_consumption
                , d.days_since_last_consumption
                {# , d1.last_date_of_consumption as last_date_of_consumption_1
                , d1.days_since_last_consumption as days_since_last_consumption_1 #}
from            {{ ref('oy_dbt_accounts_to_ckeck')}} as a
----------------
left join       {{ ref('oy_dbt_days_since_last_consumption')}} as d
on              a.Charging_ID_L0 = d.charging_id
----------------
{# left join       {{ ref('oy_dbt_days_since_last_consumption')}} as d1
on              a.Charging_ID_L0_1 = d.charging_id #}
)
select          *
                --, CASE when last_date_of_consumption >= last_date_of_consumption_1 then last_date_of_consumption else last_date_of_consumption_1 end as last_date_of_consumption
                --, CASE when last_date_of_consumption >= last_date_of_consumption_1 then days_since_last_consumption else days_since_last_consumption_1 end as days_since_last_consumption
                , case when days_since_last_consumption <= 30 then 1 else 0 end as consumption_last_30_days_flag
from            base
order by        Main_accounts