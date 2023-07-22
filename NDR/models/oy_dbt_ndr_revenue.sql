with revenue as
(
select 			ad.First_date, ad.Year, ad.Month, ad.Client_ID, ad.Client_Name, ad.business_domain
				, case 	when year(ad.First_date) = year(now()) and month(ad.First_date)>=month(now()) then null 
						when Revenue_USD is null then 0 else Revenue_USD 
				end as Revenue_USD
from 			{{ ref('oy_dbt_ndr_accounts_dates')}} as ad -- analytics.oy_dbt_ndr_accounts_dates
left join		{{ source('analytics', 'oy_v_GL_revenue_all_domains')}} as rev -- analytics.oy_v_GL_revenue_all_domains
on				ad.First_date = rev.first_date
				and ad.Client_ID = rev.Client_ID
				and ad.Client_Name = rev.Client_Account_Name
				and ad.business_domain = rev.business_domain
--order by 	ad.Client_ID, ad.business_domain, ad.first_date
),
ytd_revenue as 
(
select 			*
				, sum(Revenue_USD) over (partition by Client_ID, business_domain, year order by First_date 
											ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
				as YTD_Revenue
from 			revenue
),
lag_rev as
(
select 			*
				, lag(YTD_Revenue,12,0) over (partition by Client_ID, business_domain order by First_date) as lag_YTD_Rev
from 			ytd_revenue
order by 		Client_ID, month, year
),
ndr_rev as
(
select 			lag_rev.*
				, case when lag_YTD_Rev=0 or lag_YTD_Rev is null then 0 else YTD_Revenue end as NDR_Revenue
from 			lag_rev
),
ndr as
(
select			*
				, case when lag_YTD_Rev=0 then null else round(100*NDR_Revenue/lag_YTD_Rev,2) end as Acc_NDR
from 			ndr_rev
order by  		Client_Name, Client_ID, business_domain, first_date
),
ndr1 as
(
select 			ndr.*
				, sum(Revenue_USD) over (partition by Client_ID, business_domain, Year) as total_rev
from			ndr
),
ndr2 as
(
select 			ndr1.*
				, lag(total_rev,12,0) over (partition by Client_ID, business_domain order by First_date) as lag_total_rev
from			ndr1
),
hs as
(
select 			distinct
				odoo_id, odoo_name, hs_team, hs_manager, tcsm_manager
from 			{{ source('analytics', 'oy_v_odoo_hs_team_manager')}} -- analytics.oy_v_odoo_hs_team_manager
where 			hs_team is not null and hs_manager is not null
order by 		hs_manager
),
top40 as 
(
select 			No, "Odoo ID" 
from 			{{ source('sandbox', 'oy_sales_plans_gsheet')}} -- sandbox.oy_sales_plans_gsheet 
where 			No<>3 order by No
), 
final as
(
select 			ndr2.*
				, case when year(First_date) in(2022,2023) and total_rev>0 and lag_total_rev=0 then business_domain||' New Business'
					   else null
				  end as New_Business
				, hs.hs_team, hs.hs_manager, hs.tcsm_manager
				, case when top40.No is not NULL then 'Top 40' else NULL end as client_rnk
from			ndr2
----------------
left join		hs
on				ndr2.Client_ID = hs.odoo_id
----------------
left join		top40
on				ndr2.Client_ID = top40."Odoo ID"
)
select 			*
from 			final
--where 			total_rev<>0 or lag_total_rev<>0