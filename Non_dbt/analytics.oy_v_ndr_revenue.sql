drop view if exists analytics.oy_v_ndr_revenue
;
create view analytics.oy_v_ndr_revenue as 
select 		l.*		
			, case when NDR_22.cnt > 1 then 1 else 0 end as NDR_22
			, case when NDR_23.cnt > 1 then 1 else 0 end as NDR_23
			, top40.No
			, case when top40.No is not NULL then 'Top 40' else NULL end as client_rnk
			, tm.hs_team, tm.hs_manager
from 		analytics.oy_v_GL_revenue_all_domains as l
left join	(
				select 		Client_ID , Client_Account_Name , business_domain , count(Client_ID) as cnt
				from
							(
								select 		Client_ID ,Client_Account_Name, year, business_domain, SUM(Revenue_USD) as Revenue_USD
								from 		analytics.oy_v_GL_revenue_all_domains
								where 		Revenue_USD > 0 and (year = 2021 or year = 2022)
								group by 	1,2,3,4
								order by 	Client_Account_Name, year
							)x
				group by 	1,2,3
			) as NDR_22
on 			l.Client_ID = NDR_22.Client_ID and l.Client_Account_Name = NDR_22.Client_Account_Name
			and l.business_domain = NDR_22.business_domain
left join	(
				select 		Client_ID , Client_Account_Name , business_domain , count(Client_ID) as cnt
				from
							(
								select 		Client_ID ,Client_Account_Name, year, business_domain, SUM(Revenue_USD) as Revenue_USD
								from 		analytics.oy_v_GL_revenue_all_domains
								where 		Revenue_USD > 0 and (year = 2022 or year = 2023)
								group by 	1,2,3,4
								order by 	Client_Account_Name, year
							)x
				group by 	1,2,3
			) as NDR_23
on 			l.Client_ID = NDR_23.Client_ID and l.Client_Account_Name = NDR_23.Client_Account_Name
			and l.business_domain = NDR_23.business_domain
left join	(select No, "Odoo ID" from sandbox.oy_sales_plans_gsheet where No<>3 order by No) as top40
on			l.Client_ID = top40."Odoo ID"
left join 	(
				select 		distinct
							odoo_id, odoo_name, email, hs_team, hs_manager, hs_email
				from 		analytics.oy_v_odoo_hs_team_manager
				where 		hs_team is not null and hs_manager is not null
				order by 	hs_manager
			)as tm 
on 			tm.odoo_id = l.Client_ID 
order by 	l.Client_Account_Name
			, month , first_date
;