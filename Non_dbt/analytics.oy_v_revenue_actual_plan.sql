/*
 * GL Actual vs Plan
 */
drop view if exists analytics.oy_v_revenue_actual_plan
;
create view analytics.oy_v_revenue_actual_plan as
select 		COALESCE(l.Year , r.Year) as Year 
			, COALESCE(l.quater , r.Quater) as quater
			, COALESCE(l.month , t.month) as month
			, COALESCE(l.first_date , t.calendar_month_nk) as first_date
			--, t.month , t.calendar_month_nk
			, coalesce(r.No , r1.No) as No
			, COALESCE(l.Client_Account_Name, r.Customer_Name) as Client_Account_Name
			, l.business_domain , l.GL_Revenue_USD 
			, r.Year_rev_plan 
			, r.Quater_rev_plan 
			, r.Q1_rev_plan , r.Q2_rev_plan , r.Q3_rev_plan , r.Q4_rev_plan
			, COALESCE(l.Client_ID , r.Odoo_ID) as Client_ID
			, r.HubSpot_ID , r.Domain
from		(select * from analytics.oy_sales_plans_gsheet where No<>3) as r
full join 	(
				select 		TO_CHAR(Posting_Date, 'YYYY') as year
							, CONCAT('Q',TO_CHAR(Posting_Date, 'Q')) as quater
							, TO_CHAR(Posting_Date, 'Mon') as month
							, LAST_DAY(ADD_MONTHS(Posting_Date, -1)) + 1 as first_date
							, Client_ID
							, Client_Account_Name
							, business_domain
							, sum(Revenue_USD) as GL_Revenue_USD
				from 		standard.odoo_gl_revenue
				where 		1=1
							AND Revenue_USD > 0
							and Client_ID in (select distinct Odoo_ID from analytics.oy_sales_plans_gsheet)
				group by	1,2,3,4,5,6,7
			) as l
on 			l.Client_ID = r.Odoo_ID and l.year = r.Year and l.Quater = r.Quater 
left join	(select distinct No, Odoo_ID, Customer_Name from analytics.oy_sales_plans_gsheet where No<>3) as r1
on			l.Client_ID = r1.Odoo_ID
left join	(
				select 		distinct '2023' as calendar_year_num 
							, 'Q'||calendar_quarter_num  as quater
							, TO_CHAR(calendar_month_nk::DATE, 'Mon') as month
							, TO_DATE('2023'||right(calendar_month_nk, 6), 'YYYY-MM-DD') as calendar_month_nk
				from 		standard.dim_time
				where 		calendar_year_num <>0
				order by 	calendar_month_nk
			) as t
on			COALESCE(l.Year , r.Year) = t.calendar_year_num and COALESCE(l.quater , r.Quater) = t.quater
where 		1=1
			--and l.Client_ID in (select distinct Odoo_ID from analytics.oy_sales_plans_gsheet)	
			--and COALESCE(l.Year , r.Year) = 2023
order by 	r.No, first_date , r.Quater
;