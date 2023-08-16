{{config (materialized='table')}}
with base as
(
select 		ob.* 
			, rr.recent_selling_rate --, rr.max_ingestion_date 
			, fr.first_selling_rate
			, p.parent_selling_rate
			, coalesce(rr.charging_id, fr.charging_id) as charging_id
			, da.pd_user_account_name, da.pd_user_user_name
			, coalesce(lm.manual_odoo_name, lm.Master_Account_Name) as Master_Account_Name
			, coalesce(lm.manual_odoo_id, lm.Master_Account_ID) as Master_Account_ID
from 		{{ ref('oy_dbt_ocs_balance') }} as ob --analytics.oy_ocs__balance_1 
left join 	(	
				select 		distinct id, name , selling_rate_numeric as recent_selling_rate, charging_id
				from 		{{ ref('oy_dbt_ranking_sell_rates') }} --analytics.oy_ranking_sell_rates
				where 		rnk = cnt
			)	as rr --recent selling rates
on 			ob.id = rr.id
------
left join 	(	
				select 		distinct id, name , selling_rate_numeric as first_selling_rate, charging_id
				from 		{{ ref('oy_dbt_ranking_sell_rates') }} --analytics.oy_ranking_sell_rates 
				where 		rnk = 1
			)	as fr --first/old selling rates
on 			ob.id = fr.id
------
left join 	(	
				select 		distinct name , selling_rate_numeric as parent_selling_rate, charging_id
				from 		{{ ref('oy_dbt_ranking_sell_rates') }} --analytics.oy_ranking_sell_rates
				where 		rnk = cnt and charging_id = split_part(name, '_', 1)
			)	as p --where no match on id for subaccounts
on 			ob.name = p.name
---
left join	raw.ocs__account as ra
on 			ob.account_id = ra.id
---
left join 	(select * from standard.dim_accounts) as da
on 			ra.account_id = da.charging_id::VARCHAR2 
---
left join 	(
				select 		*
				from 		analytics.lm_v_master_account_mapping
			) as lm
on 			ra.account_id = lm.Charging_ID_L0::VARCHAR2 
---
where		1=1 
			and ob.name not ilike '%test%'
			and ob.balance_type = 2
order by 	ob.id, ob.ingestion_date
) -- circa 350 testing accounts
select 		id, parent_balance_id as parent_id
			, name, ingestion_date, SELLING_RATE, raw_selling_rate
			, recent_selling_rate, first_selling_rate, parent_selling_rate
			, coalesce(parent_selling_rate, recent_selling_rate, first_selling_rate, raw_selling_rate) as analytics_selling_rate
			, case 	when Master_Account_ID = 4112 and ingestion_date<='2023-09-30' then 0.01066667 --'Saudi Manpower Solutions Co.', Thomas will and will upload correct rates before 2023-06-14
					when Master_Account_ID = 10236 and ingestion_date<='2023-09-30' then 0.00986667 --'National Housing Company', Thomas will and will upload correct rates before 2023-06-14
					when Master_Account_ID = 4170 and ingestion_date<='2023-09-30' then  0.00960 --'Al-Qdrah for Communications and Information Technology Ltd Co. - Qdrah / Zid'				
					when Master_Account_ID = 9198 and ingestion_date<='2023-09-30' then  0.01120 --Arabian Oud Trading Company.
					when Master_Account_ID = 4154 and ingestion_date<='2023-09-30' then  0.01146667 --Takamol Holding
					when Master_Account_ID = 3183 and ingestion_date<='2023-09-30' then  0.01013333 --Careem Transportation Information Technology Company , Master_Account_ID = 3183 
					when Master_Account_ID = 10626 and ingestion_date<='2023-09-30' then 0.01243760 --Careem Networks FZ LLC (AE) , Master_Account_ID = 10626
					when name = '10002202_Saudi Arabia SMS Package' and ingestion_date<='2023-09-30' then  0.01066667 --Nakhla Information Systems Technology (SA), Master_Account_ID = 3368
					when name = '10002202_UAE All Operators' and ingestion_date<='2023-09-30' then  0.01347856 --Tamara FZE, Master_Account_ID = 9598
					when name = '20000600_Basic KSA Package with International Support' and ingestion_date<='2023-06-30' then  0.01 --Tabby FZ-LLC, Master_Account_ID = 10168
					when Master_Account_ID = 3436 and ingestion_date<='2023-09-30' then 0.00986667 -- Al-Nahdi Medical Company
					when Master_Account_ID = 3776 and ingestion_date<='2023-09-30' then 0.01026667 -- Tamkeentech - Tamkeen Technologies
					when Master_Account_ID = 3394 and ingestion_date<='2023-09-30' then 0.01013333 -- Saudi Company For Hardware - SACO
					when Master_Account_ID = 11055 and ingestion_date<='2023-09-30' then 0.01653333 -- Abdul Hadi Hamoud Al-Qahtani Foundation for Information Systems Technology
					when Master_Account_ID = 4253 and ingestion_date<='2023-06-30' then  0.03976533 -- B-Tech Trading and Distribution Co.
					when Master_Account_ID = 12164 and ingestion_date<='2023-09-30' then 0.05333333 -- Unified Real Estate Development Company
					when Master_Account_ID = 3168 and ingestion_date<='2023-09-30' then 0.0112 -- Azim Technical Financial Company Ltd.
					when Master_Account_ID = 3469 and ingestion_date<='2023-09-30' then 0.03866667 -- Lazurde Company for Jewellery
					when Master_Account_ID = 10368 and ingestion_date<='2023-09-30' then 0.01013333 -- Sondoq Alaibtikar for information and technology
					when Master_Account_ID = 3965 and ingestion_date<='2023-09-30' then 0.013333 -- Kingdom Resources Trading Company - Sendebad
					when Master_Account_ID = 3704 and ingestion_date<='2023-09-30' then 0.0105333  -- Aram Meem Trading Services Company - Toyou
					when Master_Account_ID = 3436 and ingestion_date<='2023-09-30' then 0.00986667  -- Al-Nahdi Medical Company
					else null
			  end as manual_sell_rate
			, Master_Account_Name, pd_user_user_name, pd_user_account_name, Master_Account_ID
			, charging_id
			, start_date, end_date, initial_units, units, account_id, bundle_name, status
from 		base
where 		1=1 
order by 	id, ingestion_date