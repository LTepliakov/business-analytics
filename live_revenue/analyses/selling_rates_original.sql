--#############################################################
drop view analytics.oy_ocs__balance_1
;
create view analytics.oy_ocs__balance_1 as
select 	*
		, case 	when 	split_part(selling_rate,'USD',1)::numeric(10,8) > 0 and split_part(selling_rate,'USD',1)::numeric(10,8)<>1 
						then left(split_part(selling_rate,'USD',1), 10)::numeric(10,8) 
				else 	null 
		  end as raw_selling_rate
from 	raw.ocs__balance 
where 	1=1 
		and SELLING_RATE not ilike '%SAR%'
		and balance_type=2
-----
union
select 	*
		, case 	when 	split_part(selling_rate,'SAR',1)::numeric(10,8) > 0 and split_part(selling_rate,'SAR',1)::numeric(10,8)<>1
						then left(split_part(selling_rate,'SAR',1), 10)::numeric(10,8) 
				else 	null 
		  end as raw_selling_rate
from 	raw.ocs__balance 
where 	1=1 
		and SELLING_RATE ilike '%SAR%'
		and balance_type=2
-----
union
select 	*
		, SELLING_RATE::numeric(10,8) as raw_selling_rate
from 	raw.ocs__balance 
where 	1=1 
		and SELLING_RATE is null	
		and balance_type=2
;
select * from analytics.oy_ocs__balance_1 --where SELLING_RATE is null 
order by ingestion_date desc
;
--#############################################################
/*
 * Selling rates ranking
 */
drop view if exists analytics.oy_ranking_sell_rates
;
create view analytics.oy_ranking_sell_rates as
select 		RANK () over (partition by id order by max_ingestion_date) as rnk
			, COUNT(id) over (partition by id) as cnt
			, a.*
			, da.pd_user_account_name, da.pd_user_user_name
			, lm.Master_Account_Name, lm.Master_Account_ID
from 		(
				select 		charging_id, id, name, selling_rate_numeric
							, max(ingestion_date) as max_ingestion_date 
				from 		(
								select 		a.account_id as charging_id, b.*
											, left(split_part(selling_rate,'USD',1), 8)::numeric(20,6) AS selling_rate_numeric  
								from 		raw.ocs__balance as b
								left join	raw.ocs__account as a
								on 			b.account_id = a.id
								where 		balance_type=2
											and SELLING_RATE not ilike '%SAR%'
											--and ingestion_date<='2023-04-26' 
											--the date range is chosen based on the bugfix in this Slack thread:
											--https://unifonic.slack.com/archives/C04KV5EGUDC/p1682572150715099?thread_ts=1682441533.662779&cid=C04KV5EGUDC
											--and a.account_id = 10005213
							)x
				group by 	1,2,3,4
			) as a
---
left join 		(select * from standard.dim_accounts) as da
on 				a.charging_id = da.charging_id::VARCHAR2 
---
left join 		(select * from analytics.lm_v_master_account_mapping) as lm
on 				a.charging_id = lm.Charging_ID_L0::VARCHAR2 
---
where  		1=1
			and selling_rate_numeric<>1 and selling_rate_numeric<>0 and selling_rate_numeric is not null 
order by 	id , max_ingestion_date
;
--#############################################################
/*
 * Cleansing
 */
drop view if exists analytics.oy_v_selling_rates_imputed
;
create view analytics.oy_v_selling_rates_imputed as
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
from 		analytics.oy_ocs__balance_1 as ob
left join 	(	
				select 		distinct id, name , selling_rate_numeric as recent_selling_rate, charging_id
				from 		analytics.oy_ranking_sell_rates 
				where 		rnk = cnt
			)	as rr --recent selling rates
on 			ob.id = rr.id
------
left join 	(	
				select 		distinct id, name , selling_rate_numeric as first_selling_rate, charging_id
				from 		analytics.oy_ranking_sell_rates 
				where 		rnk = 1
			)	as fr --first/old selling rates
on 			ob.id = fr.id
------
left join 	(	
				select 		distinct name , selling_rate_numeric as parent_selling_rate, charging_id
				from 		analytics.oy_ranking_sell_rates
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
			, case 	when Master_Account_ID = 4112 and ingestion_date<='2023-07-31' then 0.01066667 --'Saudi Manpower Solutions Co.', Thomas will and will upload correct rates before 2023-06-14
					when Master_Account_ID = 10236 and ingestion_date<='2023-06-14' then 0.00986667 --'National Housing Company', Thomas will and will upload correct rates before 2023-06-14
					when Master_Account_ID = 4170 and ingestion_date<='2023-06-14' then  0.00960 --'Al-Qdrah for Communications and Information Technology Ltd Co. - Qdrah / Zid'				
					when Master_Account_ID = 9198 and ingestion_date<='2023-07-31' then  0.01120 --Arabian Oud Trading Company.
					when Master_Account_ID = 4154 and ingestion_date<='2023-07-31' then  0.01146667 --Takamol Holding
					when Master_Account_ID = 3183 and ingestion_date<='2023-06-14' then  0.01013333 --Careem Transportation Information Technology Company , Master_Account_ID = 3183 
					when Master_Account_ID = 10626 and ingestion_date<='2023-06-14' then 0.01243760 --Careem Networks FZ LLC (AE) , Master_Account_ID = 10626
					when name = '10002202_Saudi Arabia SMS Package' and ingestion_date<='2023-06-30' then  0.01066667 --Nakhla Information Systems Technology (SA), Master_Account_ID = 3368
					when name = '10002202_UAE All Operators' and ingestion_date<='2023-06-30' then  0.01347856 --Tamara FZE, Master_Account_ID = 9598
					when name = '20000600_Basic KSA Package with International Support' and ingestion_date<='2023-06-30' then  0.01 --Tabby FZ-LLC, Master_Account_ID = 10168
					when Master_Account_ID = 3436 and ingestion_date<='2023-06-30' then 0.00986667 -- Al-Nahdi Medical Company
					when Master_Account_ID = 3776 and ingestion_date<='2023-07-31' then 0.01026667 -- Tamkeentech - Tamkeen Technologies
					when Master_Account_ID = 3394 and ingestion_date<='2023-07-31' then 0.01013333 -- Saudi Company For Hardware - SACO
					when Master_Account_ID = 11055 and ingestion_date<='2023-07-31' then 0.01653333 -- Abdul Hadi Hamoud Al-Qahtani Foundation for Information Systems Technology
					when Master_Account_ID = 4253 and ingestion_date<='2023-07-31' then  0.03976533 -- B-Tech Trading and Distribution Co.
					when Master_Account_ID = 12164 and ingestion_date<='2023-07-31' then 0.05333333 -- Unified Real Estate Development Company
					when Master_Account_ID = 3168 and ingestion_date<='2023-07-31' then 0.0112 -- Azim Technical Financial Company Ltd.
					when Master_Account_ID = 3469 and ingestion_date<='2023-07-31' then  0.03866667 -- Lazurde Company for Jewellery
					when Master_Account_ID = 10368 and ingestion_date<='2023-07-31' then  0.01013333 -- Sondoq Alaibtikar for information and technology
					when Master_Account_ID = 3965 and ingestion_date<='2023-07-31' then 0.013333 -- Kingdom Resources Trading Company - Sendebad
					when Master_Account_ID = 3704 and ingestion_date<='2023-07-31' then 0.0105333  -- Aram Meem Trading Services Company - Toyou
					else null
			  end as manual_sell_rate
			, Master_Account_Name, pd_user_user_name, pd_user_account_name, Master_Account_ID
			, charging_id
			, start_date, end_date, initial_units, units, account_id, bundle_name, status
from 		base
where 		1=1 
order by 	id, ingestion_date
;





