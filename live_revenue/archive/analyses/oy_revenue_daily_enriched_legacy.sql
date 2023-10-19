{{config (materialized='table')}} 

-- drop table if exists analytics.oy_revenue_daily_enriched
-- ;
--create table analytics.oy_revenue_daily_enriched as
with base as
(
select 		COALESCE (ca.rep_month, gl.first_date) as report_month
			, ca.*
			, case when top40."Customer Name" is null then null else 'Top 40' end as client_rank
			, case 	when bundle_type='mb' then charge 
					else ca.charge*sr.raw_selling_rate 
			  end as raw_revenue
			, case 	when bundle_type='mb' then charge 
					else ca.charge*COALESCE(sr.manual_sell_rate, sr.analytics_selling_rate, srp.parent_selling_rate)
			  end as analytics_revenue
			, case 	when ca.units>0 and sr.manual_sell_rate is not null then ca.charge*sr.manual_sell_rate
					when ca.units>0 and ABS(ca.cdr_selling_rate/COALESCE(srp.parent_selling_rate, sr.analytics_selling_rate)-1)>0.2 and bundle_type='package'
							then ca.charge*COALESCE(sr.analytics_selling_rate, srp.parent_selling_rate) --to address 1.0USD use case e.g. ELM
					when ca.units>0 and (cdr_revenue = 0 or cdr_revenue is null or SELLING_RATE='1.0USD') and bundle_type='package'
							then ca.charge*COALESCE(sr.analytics_selling_rate, srp.parent_selling_rate)
					when lmm.Master_Account_Name='Mobile Telecommunications Company - Zain Saudi Arabia' and date_nk<='2023-06-30' and ca.bundle_type = 'mb'
							then 0.0109*ca.units
					--0.0109 4-digit constrained which will be changed to more than 4 digit in June by Sven
					--https://unifonic.slack.com/archives/C04KV5EGUDC/p1684654979174349?thread_ts=1684328640.491369&cid=C04KV5EGUDC
					--https://unifonic.slack.com/archives/C04KV5EGUDC/p1684478079173099?thread_ts=1684328640.491369&cid=C04KV5EGUDC
					else cdr_revenue
			  end as final_revenue
			, gl.GL_Revenue, gl_open.GL_Open_Acc_Revenue, gl_adj.GL_Revenue_adjustm, gl_forfeit.GL_Revenue_forfeit
			, sr.SELLING_RATE
			, sr.raw_selling_rate, sr.analytics_selling_rate, sr.manual_sell_rate, srp.parent_selling_rate
			, COALESCE(ca.manual_odoo_id , lmm.Master_Account_ID, gl.Client_ID::VARCHAR2) as Master_Account_ID --Separate Tamara from Nakhla
			, COALESCE(ca.manual_odoo_name, lmm.Master_Account_Name, gl.Client_Account_Name) as Master_Account_Name --Separate Tamara from Nakhla
			, lmm.Master_Account_Origin, sa.Account_Type
from		(
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
				from 		aggregate.fact_sms_consumption_aggregate as ca
				left join 	standard.dim_accounts as da
				on 			ca.account_id = da.charging_id
				where 		ca.event_type = 'default.sms'
							and date(hour_nk) >= '2023-01-01'
							--and ca.account_id = 114
				group by	1,2,3,4,5,6,7,8,9,10
				order by 	date(ca.hour_nk) desc
			) as ca 
-------
left join 	(
				select 		* 
				from 		analytics.oy_v_selling_rates_imputed
				where 		analytics_selling_rate is not null
				order by 	id, ingestion_date
			) as sr
on			1=1
			and ca.charging_id = sr.charging_id::INTEGER
			and RTRIM(ca.bundle_name, ' ') = sr.name
			and ca.date_nk = sr.ingestion_date
-------Fallback logic for subaccounts 'where charging_id is null' use-case
left join 	(
				select distinct name, parent_selling_rate from analytics.oy_v_selling_rates_imputed
				where 	analytics_selling_rate is not null
			) as srp
on			1=1
			and RTRIM(ca.bundle_name, ' ') = srp.name
-------		
left join 	(
				select 		distinct a.ingestion_date, a.name, a.SELLING_RATE as SELLING_RATE_ORG, b.account_id as charging_id
				from 		raw.ocs__balance as a
				left join 	raw.ocs__account as b
				on 			a.account_id = b.id::VARCHAR2
				where 		1=1 and a.SELLING_RATE not ilike '%SAR%' and split_part(a.name,'_',1)<>'mb'
				order by 	ingestion_date desc
			) as aa
on			1=1
			and ca.charging_id = aa.charging_id::INTEGER
			and RTRIM(ca.bundle_name, ' ') = aa.name
			and ca.date_nk = aa.ingestion_date
-------
left join 	(	
				select 		Charging_ID_L0
							, COALESCE(manual_odoo_id, Master_Account_ID) as Master_Account_ID
							, COALESCE(manual_odoo_name, Master_Account_Name) as Master_Account_Name
							, Master_Account_Origin
				from 		analytics.lm_v_master_account_mapping
			) as lmm
on 			ca.charging_id = lmm.Charging_ID_L0
-------
left join	(select * from analytics.oy_special_accounts) as sa
on 			ca.charging_id = sa.charging_id
-------
full join	(
				select 		first_date, Client_ID , Client_Account_Name, sum(Revenue_USD) as GL_Revenue
--							, SUM(case 	when Client_ID=3368 and GL_Reference='SMS Revenue Recognition May 2023' then 0 -- Exclude 'Nakhla Information Systems Technology' May adjustment
--										else Revenue_USD end
--								 ) as GL_Revenue
				from 		analytics.oy_v_GL_revenue_reference --analytics.oy_v_GL_revenue_all_domains replaced
				where 		1=1 
							and year >= 2023
							and business_domain = 'SMS'
							and Client_ID is not NULL 
							and GL_Reference not ilike '%Forfeiture%'
							and GL_Reference not ilike '%Adjustments%'
							and GL_Reference not ilike '%open account%'
							--and Client_ID=3368
							group by 	1,2,3
				order by 	Client_ID , first_date desc
			) as gl
on			ca.rep_month = gl.first_date and coalesce(ca.manual_odoo_id ,lmm.Master_Account_ID) = gl.Client_ID
-------
full join	(
				select 		first_date, Client_ID , Client_Account_Name, sum(Revenue_USD) as GL_Open_Acc_Revenue
				from 		analytics.oy_v_GL_revenue_reference --analytics.oy_v_GL_revenue_all_domains replaced
				where 		1=1 
							and year >= 2023
							and business_domain = 'SMS'
							and Client_ID is not NULL 
							and GL_Reference ilike '%open account%'
							--and Client_ID=3368
							group by 	1,2,3
				order by 	Client_ID , first_date desc
			) as gl_open
on			ca.rep_month = gl_open.first_date and coalesce(ca.manual_odoo_id ,lmm.Master_Account_ID) = gl_open.Client_ID
-------
full join	(
				select 		first_date, Client_ID , Client_Account_Name, sum(Revenue_USD) as GL_Revenue_adjustm
				from 		analytics.oy_v_GL_revenue_reference --analytics.oy_v_GL_revenue_all_domains replaced
				where 		1=1 
							and year >= 2023
							and business_domain = 'SMS'
							and Client_ID is not NULL 
							and GL_Reference ilike '%Adjustments%'
							--and Client_ID=3368
							group by 	1,2,3
				order by 	Client_ID , first_date desc
			) as gl_adj
on			ca.rep_month = gl_adj.first_date and coalesce(ca.manual_odoo_id ,lmm.Master_Account_ID) = gl_adj.Client_ID
-------
full join	(
				select 		first_date, Client_ID , Client_Account_Name, sum(Revenue_USD) as GL_Revenue_forfeit
				from 		analytics.oy_v_GL_revenue_reference --analytics.oy_v_GL_revenue_all_domains replaced
				where 		1=1 
							and year >= 2023
							and business_domain = 'SMS'
							and Client_ID is not NULL 
							and GL_Reference ilike '%Forfeiture%'
							--and Client_ID=3368
							group by 	1,2,3
				order by 	Client_ID , first_date desc
			) as gl_forfeit
on			ca.rep_month = gl_forfeit.first_date and coalesce(ca.manual_odoo_id ,lmm.Master_Account_ID) = gl_forfeit.Client_ID
-------
left join 	(select * from sandbox.oy_sales_plans_gsheet where No<>3 order by No) as top40
on 			coalesce(ca.manual_odoo_id ,lmm.Master_Account_ID) = top40."Odoo ID"
-------
where 		1=1 and report_month is not null
order by 	ca.charging_id, ca.date_nk
)
select 		report_month
			, date_nk
			, charging_id
			, pd_user_id
			, pd_user_account_name
			, Master_Account_ID
			, Master_Account_Name
			, client_rank
			, Master_Account_Origin
			, case when Account_Type is null then 'Other' else Account_Type end as Account_Type
			, bundle_name
			, pd_user_country
			, bundle_type
			, units
			, charge
			, round(cdr_revenue,2) as cdr_revenue
			, round(raw_revenue,2) as raw_revenue
			, round(analytics_revenue,2) as analytics_revenue
			, round(final_revenue,2) as final_revenue
			, round(sum(charge) over (partition by Master_Account_ID, report_month),2) as final_charge_month
			, round(sum(final_revenue) over (partition by Master_Account_ID, report_month),2) as final_revenue_month
			, GL_Revenue
			, GL_Open_Acc_Revenue
			, GL_Revenue_adjustm
			, GL_Revenue_forfeit
			, round(sum(final_revenue) over (partition by Master_Account_ID, report_month)-coalesce(GL_Open_Acc_Revenue, GL_Revenue), 2) as var
			, case 	when (GL_Revenue is null or GL_Revenue=0) and (GL_Open_Acc_Revenue is null or GL_Open_Acc_Revenue=0) then null 
					ELSE round(100*((sum(final_revenue) over (partition by Master_Account_ID, report_month))/coalesce(GL_Open_Acc_Revenue, GL_Revenue) - 1), 2)  
			  end as pct_var
			, round(sum(charge) over (partition by Master_Account_ID, report_month, charging_id),2) as final_charge_ch_id_month
			, case 	when charge=0 or charge is null then null 
					when bundle_type = 'mb' then round(final_revenue/units,6)
			  		else round(final_revenue/charge,6) 
			  end as final_selling_rate			 
			, SELLING_RATE
			, raw_selling_rate
			, cdr_selling_rate
			, analytics_selling_rate
			, manual_sell_rate
			, parent_selling_rate
			, tm.hs_team , tm.hs_manager, tm.tcsm_manager
from 		base
left join 	(
				select 		distinct
							odoo_id, odoo_name, email, hs_team, hs_manager, hs_email, tcsm_manager
				from 		analytics.oy_v_odoo_hs_team_manager
				where 		hs_team is not null and hs_manager is not null
				order by 	hs_manager
			)as tm 
on 			tm.odoo_id = base.Master_Account_ID
where 		1=1 --and charging_id = 20002456 --Saudi Abyat for Building Materials OCS anomaly https://docs.google.com/spreadsheets/d/1ryZh7N_7zbnhgZIaLkCv2u2TzWuXBn1CY02ZKPbZEtY/edit?usp=sharing
order by 	charging_id, date_nk