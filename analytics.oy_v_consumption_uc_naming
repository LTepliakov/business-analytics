drop view if exists analytics.oy_v_consumption_uc_naming
;
create view analytics.oy_v_consumption_uc_naming as
select 		l.*, r.account_name as uc_account 
from 		(
				select 		LAST_DAY(ADD_MONTHS(hour_nk, -1)) + 1 as first_date
				 			, TO_CHAR(hour_nk, 'YYYY') as year
							, TO_CHAR(hour_nk, 'Mon') as month
				 			--, TO_DATE(TO_CHAR(hour_nk), 'YYYY-MM-DD') as date
				 			, trim(split_part(bundle_name, '_', 2)) as bundle 
				 			, case 	when trim(split_part(bundle_name, '_', 2)) is null then NULL
									when trim(split_part(bundle_name, '_', 2)) = 'mb' then 'mb'
									else 'package'
									end as plan
							, account_id
							--, NULL::varchar(200) as package
							, SUM(units) as units, SUM(charge) as charge
				from 		aggregate.fact_sms_consumption_aggregate
				where		1=1
							and result = 'SUCCESS'
							and action = 'DIRECT_DEBIT'
				group by	1,2,3,4,5,6
			) as l
left join 	(
				select 		distinct 
							l.charging_id 
							--, r.*
							, r.account_name
				from 		raw.unifonic_cloud__account as l
				left join 	standard.dim_pd_users as r
				on 			l.provisioning_id = r.pd_user_id 
				--where 		pd_user_id is null
			) as r
on 			l.account_id = r.charging_id
;