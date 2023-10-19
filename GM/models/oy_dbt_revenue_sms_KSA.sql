{{ config(materialized='view')}}

with base as 
(
select 		    c.*
                , case when b.Master_Account_ID is not null then '1' else 0 end as no_mb_client
                , case when i.Master_Account_ID is not null then '1' else 0 end as sell_rate_invalid
from            {{ source('analytics','oy_dbt_consumption_agg_sms_KSA')}} as c
----------------
left join       (select distinct Master_Account_ID from {{ ref('oy_dbt_no_mb_clients')}}) as b 
on              c.Master_Account_ID = b.Master_Account_ID
----------------
left join       (select distinct Master_Account_ID from {{ ref('oy_dbt_clients_with_invalid_selling_rate')}}) as i 
on              c.Master_Account_ID = i.Master_Account_ID
where           1=1
                and c.date_nk > '2023-08-01' 
)
select          * 
from            base
where           1=1
                and no_mb_client = 1
                and sell_rate_invalid = 0