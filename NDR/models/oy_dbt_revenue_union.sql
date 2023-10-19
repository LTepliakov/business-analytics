{{ config(materialized='view') }}

(
select 			ad.First_date, ad.Year, ad.Month, ad.Client_ID, ad.Client_Name, ad.business_domain
				, case 	when year(ad.First_date) = year(now()) and month(ad.First_date)>=month(now()) then null 
						when Revenue_USD is null then 0 else Revenue_USD 
				end as Revenue_USD
from 			{{ ref('oy_dbt_ndr_accounts_dates')}} as ad -- analytics.oy_dbt_ndr_accounts_dates
left join		{{ source('analytics', 'oy_dbt_GL_revenue_all_domains')}} as rev -- analytics.oy_v_GL_revenue_all_domains
on				ad.First_date = rev.first_date
				and ad.Client_ID = rev.Client_ID
				and ad.Client_Name = rev.Client_Account_Name
				and ad.business_domain = rev.business_domain
where 			1=1 
                and ad.Client_ID is not null 
                and rev.Client_ID is not null
)
UNION
(
select 			ad.First_date, ad.Year, ad.Month, ad.Client_ID, ad.Client_Name, ad.business_domain
				, case 	when year(ad.First_date) = year(now()) and month(ad.First_date)>=month(now()) then null 
						when rev.Revenue_USD is null then 0 else Revenue_USD 
				end as Revenue_USD
from 			{{ ref('oy_dbt_ndr_accounts_dates')}} as ad
left join		{{ source('analytics', 'oy_dbt_GL_revenue_all_domains')}} as rev
on				ad.First_date = rev.first_date
				and ad.business_domain = rev.business_domain
where 			1=1 
                and ad.Client_ID is null 
                and rev.Client_ID is null 
                and rev.Revenue_USD<>0
)