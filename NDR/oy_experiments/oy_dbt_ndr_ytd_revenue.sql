with revenue as
(
select 		ad.First_date, ad.Year, ad.Month, ad.Client_ID, ad.Client_Account_Name, ad.business_domain
			, case when Revenue_USD is null then 0 else Revenue_USD end as Revenue_USD
from 		{{ ref('oy_dbt_ndr_accounts_dates')}} as ad
left join	{{ source('analytics', 'oy_v_GL_revenue_all_domains')}} as rev
on			ad.First_date = rev.first_date
			and ad.Client_ID = rev.Client_ID
			and ad.Client_Account_Name = rev.Client_Account_Name
			and ad.business_domain = rev.business_domain
--order by 	ad.Client_ID, ad.business_domain, ad.first_date
),
ytd_revenue as 
(
select 		*
			, sum(Revenue_USD) over (partition by Client_ID, business_domain, year order by First_date 
										ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as YTD_Revenue
from 		revenue
)
select 		*
			, lag(YTD_Revenue,12,0) over (partition by Client_ID, business_domain order by First_date) as lag_YTD_Rev
			, case when YTD_revenue>0 then 1 else 0 end as YTD_flag
from 		ytd_revenue
order by 	Client_ID, month, year