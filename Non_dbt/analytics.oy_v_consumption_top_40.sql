DROP VIEW IF EXISTS analytics.oy_v_consumption_top_40
;
create view analytics.oy_v_consumption_top_40 as
select 		*
from		analytics.oy_map_uc_odoo_top_40 as l --Top 40 accounts mapping except zain and some others
left join 	(
				select 		LAST_DAY(ADD_MONTHS(hour_nk, -1)) + 1 as first_date
				 			, TO_CHAR(hour_nk, 'YYYY') as year
							, TO_CHAR(hour_nk, 'Mon') as month
				 			, TO_DATE(TO_CHAR(hour_nk), 'YYYY-MM-DD') as date
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
							--and account_id is null -- checking that there is no account_id = null
				group by	1,2,3,4,5,6,7
			) as r
on 			l.charging_id = r.account_id
order by 	l.No , r.date
;