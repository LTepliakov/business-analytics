/*
 * Upload csv data from https://docs.google.com/spreadsheets/d/14IEfGViFArIVjECGCohNkXShrKvt4tqFQRk9PE8MnoM/edit?usp=sharing
 */
drop table if exists sandbox.Hanaa_csv
;
select * from sandbox.Hanaa_csv order by No
;
drop table if exists sandbox.oy_sales_plans_gsheet
;
select * into sandbox.oy_sales_plans_gsheet from sandbox.Hanaa_csv where No is not null order by No
;
select * from sandbox.oy_sales_plans_gsheet order by No
;
/*
 * analytics.oy_sales_plans_gsheet
 */
drop table if exists analytics.oy_sales_plans_gsheet
;
create table analytics.oy_sales_plans_gsheet as
select 		No, "Customer Name" as Customer_Name
			, '2023' AS Year
			, "Total Year" as Year_rev_plan
			, Quater
			, Quater_rev_plan as Quater_rev_plan
			, case when Quater = 'Q1' then Q1 else null end as Q1_rev_plan
			, case when Quater = 'Q2' then Q2 else null end as Q2_rev_plan
			, case when Quater = 'Q3' then Q3 else null end as Q3_rev_plan
			, case when Quater = 'Q4' then Q4 else null end as Q4_rev_plan
			, "HubSpot ID" as HubSpot_ID
			, "Odoo ID" as Odoo_ID
			, Domain
from
			(
			select *, 'Q1' as Quater, Q1 as Quater_rev_plan from sandbox.oy_sales_plans_gsheet
			union all
			select *, 'Q2' as Quater, Q2 as Quater_rev_plan from sandbox.oy_sales_plans_gsheet
			union all
			select *, 'Q3' as Quater, Q3 as Quater_rev_plan from sandbox.oy_sales_plans_gsheet
			union all
			select *, 'Q4' as Quater, Q4 as Quater_rev_plan from sandbox.oy_sales_plans_gsheet
			order by Quater, No
			)x
order by Quater, No
;
update analytics.oy_sales_plans_gsheet set Customer_Name = 'Mobile Telecommunications Company - Zain Saudi Arabia'
where No = 2
;