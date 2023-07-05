--#############################################################
/*
 * Sanity check
 */
select * from analytics.oy_revenue_daily_enriched
where 		1=1
			and Master_Account_ID = 10626--10424--3479--3368--3436--3331--10168--9598--3751--10626--3510--3183--4154--4170--3335--4298--10236--4112
			--and charging_id = 20002456
			--and Master_Account_Name ilike '%careem%'--'%Tamwily%'--'%ELM Company%'--'Mobile Telecommunications Company - Zain Saudi Arabia'--'Alinma bank'
			and report_month = '2023-05-01'
			--and bundle_type <> 'mb'
order by 	charging_id, date_nk desc
;
select 		*, final_revenue - cdr_revenue as diff 
from 		analytics.oy_revenue_daily_enriched
where 		bundle_type='package' 
			--and SELLING_RATE='1.0USD'
order by 	final_revenue - cdr_revenue 
;
select 		* 
from 		raw.odoo__res_partner orp 
where 		1=1 and name ilike '%careem%'
;

select 		*
from 		standard.dim_accounts
where 		erp_id=10626
;
select 		*
from 		analytics.oy_v_GL_revenue_all_domains
where 		Client_ID = 3368 and first_date = '2023-05-01'
;
select 		*
from 		standard.odoo_gl_revenue
where 		1=1 --and Client_Account_Name ilike '%King Abdullah Bin Abdulaziz University Hospital%'
			and Client_ID = 4252
			and YEAR(Posting_Date) = 2023
order by 	Posting_Date desc
;
select 		*
from 		analytics.oy_v_GL_profit_loss 
where 		1=1 --and Client_Account_Name ilike '%Tamwily%'
			and Client_ID = 4252 
			and YEAR(Posting_Date) = 2023
			and MONTH(Posting_Date) = 5
			and business_domain = 'SMS'
order by 	Posting_Date desc
;
select 		Client_ID , Client_Account_Name , sum(Revenue_USD) as Revenue_USD 
from
(
select 		*
from 		analytics.oy_v_GL_profit_loss 
where 		1=1 
			and GL_Accounting_Name_Group = 'income'
			and GL_Accounting_Name = 'SMS Revenue'
			--and GL_Reference ilike '%forfeiture%'
			and MONTH(Posting_Date) >= 4 and YEAR(Posting_Date) = 2023
)x
group by 	1,2
order by 	Revenue_USD desc
;
select 		* 
from 		analytics.oy_v_GL_revenue_reference
where 		1=1
			and GL_Accounting_Name = 'SMS Revenue'
			and GL_Reference in 
							(
								'SMS Revenues adjustments - May 2023'
								--, 'SMS Revenues May 2023 - Open accounts'
								--, 'SMS Revenue Recognition May2023 - Open accounts'
								--, 'SMS Revenue Recognition May 2023 - Open accounts'
								, 'SMS Revenue Recognition May 2023 - Forfeiture'
								--, 'SMS Revenue Recognition May 2023'
								--, 'SMS Revenue Recognition Apr 2023 - Open accounts'
								, 'SMS Revenue Recognition Apr 2023 - Forfeiture'
								--, 'SMS Revenue Recognition Apr 2023'
								, 'SMS Revenue Adjustments as of Apr 2023'
								, 'SMS Revenue Adjustments Mar & Apr 2023'
							)
order by 	Client_Account_Name 
;
select * from analytics.oy_v_GL_revenue_all_domains 
;



--#############################################################
/*
 * Subaccouts
 */
select 		name, charging_id, count(name) as cnt
from
			(
			select distinct name, charging_id , analytics_selling_rate from analytics.oy_selling_rates_imputed
			)x 
group by 	1,2
order by 	count(name) desc, charging_id
;
select charging_id , * from analytics.oy_selling_rates_imputed
where name='10000099_Saudi Arabia SMS Package'--'20002492_Saudi Arabia SMS Package'
order by id, ingestion_date desc

