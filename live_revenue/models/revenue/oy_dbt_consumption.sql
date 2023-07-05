{{config (materialized='table')}} 

select 		LAST_DAY(ADD_MONTHS(hour_nk, -1)) + 1 as rep_month
            , date(ca.hour_nk) as date_nk
            , ca.account_id as charging_id
            , da.pd_user_id
            , da.pd_user_account_name
            , ca.bundle_name
            , da.pd_user_country
            , case 	when split_part(trim(ca.bundle_name), '_', 2)='mb' then 'mb' else 'package' end as bundle_type
            , case 	when ca.bundle_name in ('10002202_UAE All Operators', '10002202_mb') then '9598' -- Tamara
                    when ca.bundle_name in ('20000600_UAE All Operators') then '10168' -- Tabby
                    when ca.account_id = 114 then '3436' -- Nahdi Medical Company Guest Care, missing erp_id in standard.dim_accounts. When Thomas upload Batch 5 then delete this line of code
                    else null 
                end as manual_odoo_id
            , case 	when ca.bundle_name in ('10002202_UAE All Operators', '10002202_mb') then 'Tamara FZE' -- Tamara
                    when ca.bundle_name in ('20000600_UAE All Operators') then 'Tabby FZ-LLC' -- Tabby
                    else null 
                end as manual_odoo_name
            , sum(units) as units, sum(charge) as charge, sum(revenue) as cdr_revenue
            , case 	when sum(units)=0 then null
                    when split_part(trim(ca.bundle_name), '_', 2)='package' then ROUND(sum(revenue)/sum(charge),8)
                    else ROUND(sum(revenue)/sum(units),8) 
                end as cdr_selling_rate
from 		{{ source('aggregate', 'fact_sms_consumption_aggregate')}} as ca --aggregate.fact_sms_consumption_aggregate 
left join 	{{ source('standard', 'dim_accounts') }} as da --standard.dim_accounts as da
on 			ca.account_id = da.charging_id
where 		ca.event_type = 'default.sms'
            and date(hour_nk) >= '2023-01-01'
            --and ca.account_id = 114
group by	1,2,3,4,5,6,7,8,9,10
order by 	date(ca.hour_nk) desc