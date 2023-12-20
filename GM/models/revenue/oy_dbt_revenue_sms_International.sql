{{ config(materialized='table')}}

with base as 
(
select 		    c.*
                , case when b.Master_Account_ID is not null then '1' else 0 end as no_mb_client
                , case when i.Master_Account_ID is not null then '1' else 0 end as sell_rate_invalid
from            {{ source('analytics', 'oy_dbt_consumption_agg_sms_International_acc_level')}} as c
----------------
left join       (select distinct Master_Account_ID from {{ ref('oy_dbt_no_mb_clients_International')}}) as b 
on              c.Master_Account_ID = b.Master_Account_ID
----------------
left join       (select distinct Master_Account_ID from {{ ref('oy_dbt_clients_with_invalid_selling_rate_International')}}) as i 
on              c.Master_Account_ID = i.Master_Account_ID
where           1=1
                and c.date_nk >= '2023-08-01' 
                --and c.date_nk <= '2023-10-10' -- this filter is because from '2023-10-12' Alinma bank started to consume mb. I'll remove this filter later.
				--and c.cost_id is not null -- to exclude failovers
)
select          * 
from            base
where           1=1
                --and no_mb_client = 1
                and sell_rate_invalid = 0