{{ config (materialized='table', docs={'node_color':'black'}) }} 

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
			, case 	when ca.bundle_type = 'mb' and lmm.Master_Account_Name='Mobile Telecommunications Company - Zain Saudi Arabia' and ca.date_nk<'2023-07-01'
							then 0.0109*ca.units
					when ca.bundle_type = 'mb' and lmm.Master_Account_Name='Mobile Telecommunications Company - Zain Saudi Arabia' and ca.date_nk>='2023-07-01'
							then 0.01*ca.units
					when ca.bundle_type = 'mb' then ca.charge
					when ca.units>0 and sr.manual_sell_rate is not null then ca.charge*sr.manual_sell_rate
					when ca.bundle_type='package' and ca.units>0 and ABS(ca.cdr_selling_rate/COALESCE(srp.parent_selling_rate, sr.analytics_selling_rate)-1)>0.2
							then ca.charge*COALESCE(sr.analytics_selling_rate, srp.parent_selling_rate) --to address 1.0USD use case e.g. ELM
					when ca.bundle_type='package' and ca.units>0 and SELLING_RATE='1.0USD'
							then ca.charge*COALESCE(sr.analytics_selling_rate, srp.parent_selling_rate)
					--0.0109 4-digit constrained which will be changed to more than 4 digit in June by Sven
					--https://unifonic.slack.com/archives/C04KV5EGUDC/p1684654979174349?thread_ts=1684328640.491369&cid=C04KV5EGUDC
					--https://unifonic.slack.com/archives/C04KV5EGUDC/p1684478079173099?thread_ts=1684328640.491369&cid=C04KV5EGUDC
					else ca.charge*COALESCE(sr.manual_sell_rate, sr.analytics_selling_rate, srp.parent_selling_rate)
			  end as final_revenue
			, gl.GL_Revenue, gl_open.GL_Open_Acc_Revenue, gl_adj.GL_Revenue_adjustm, gl_forfeit.GL_Revenue_forfeit
			, sr.SELLING_RATE
			, sr.raw_selling_rate, sr.analytics_selling_rate, sr.manual_sell_rate, srp.parent_selling_rate
			, COALESCE(ca.manual_odoo_id , lmm.Master_Account_ID, gl.Client_ID::VARCHAR2) as Master_Account_ID --Separate Tamara from Nakhla
			, COALESCE(ca.manual_odoo_name, lmm.Master_Account_Name, gl.Client_Account_Name) as Master_Account_Name --Separate Tamara from Nakhla
			, lmm.Master_Account_Origin, sa.Account_Type
from		{{ ref('oy_dbt_consumption')}} as ca 
-------
left join 	(
				select 		* 
				from 		{{ ref('oy_dbt_selling_rates_imputed') }} --analytics.oy_v_selling_rates_imputed --
				where 		analytics_selling_rate is not null
				order by 	id, ingestion_date
			) as sr
on			1=1
			and ca.charging_id = sr.charging_id::INTEGER
			and RTRIM(ca.bundle_name, ' ') = sr.name
			and ca.date_nk = sr.ingestion_date
-------Fallback logic for subaccounts 'where charging_id is null' use-case
left join 	(
				select 		distinct name, parent_selling_rate 
				from 		{{ ref('oy_dbt_selling_rates_imputed')}} --analytics.oy_v_selling_rates_imputed --
				where 		analytics_selling_rate is not null
			) as srp
on			1=1
			and RTRIM(ca.bundle_name, ' ') = srp.name
-------		
left join 	(
				select 		distinct a.ingestion_date, a.name, a.SELLING_RATE as SELLING_RATE_ORG, b.account_id as charging_id
				from 		{{ source('raw', 'ocs__balance')}} as a --raw.ocs__balance
				left join 	{{ source('raw', 'ocs__account')}} as b --raw.ocs__account
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
				from 		{{ source('analytics', 'lm_v_master_account_mapping')}} --analytics.lm_v_master_account_mapping
			) as lmm
on 			ca.charging_id = lmm.Charging_ID_L0
-------
left join	{{ ref('oy_dbt_special_accounts')}} as sa --analytics.oy_special_accounts
on 			ca.charging_id = sa.charging_id
-------
full join	{{ ref('oy_dbt_GL_revenue')}} as gl
on			ca.rep_month = gl.first_date and coalesce(ca.manual_odoo_id ,lmm.Master_Account_ID) = gl.Client_ID
-------
full join	{{ ref('oy_dbt_GL_revenue_open_acc')}} as gl_open
on			ca.rep_month = gl_open.first_date and coalesce(ca.manual_odoo_id ,lmm.Master_Account_ID) = gl_open.Client_ID
-------
full join	{{ ref('oy_dbt_GL_revenue_adjustments')}} as gl_adj
on			ca.rep_month = gl_adj.first_date and coalesce(ca.manual_odoo_id ,lmm.Master_Account_ID) = gl_adj.Client_ID
-------
full join	{{ ref('oy_dbt_GL_revenue_forfeiture')}} as gl_forfeit
on			ca.rep_month = gl_forfeit.first_date and coalesce(ca.manual_odoo_id ,lmm.Master_Account_ID) = gl_forfeit.Client_ID
-------
left join 	(
				select 		* 
				from 		{{ ref('oy_dbt_sales_plans_gsheet')}} --sandbox.oy_sales_plans_gsheet 
				where		No<>3 order by No
			) as top40
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
			, case when Account_Type is null then 'Client Account' else Account_Type end as Account_Type
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
				from 		{{ source('analytics', 'oy_v_odoo_hs_team_manager')}} --analytics.oy_v_odoo_hs_team_manager
				where 		hs_team is not null and hs_manager is not null
				order by 	hs_manager
			)as tm 
on 			tm.odoo_id = base.Master_Account_ID
where 		1=1 --and charging_id = 20002456 --Saudi Abyat for Building Materials OCS anomaly https://docs.google.com/spreadsheets/d/1ryZh7N_7zbnhgZIaLkCv2u2TzWuXBn1CY02ZKPbZEtY/edit?usp=sharing
order by 	charging_id, date_nk