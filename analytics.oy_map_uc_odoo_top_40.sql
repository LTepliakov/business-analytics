drop table if exists analytics.oy_map_uc_odoo_top_40
;
create table analytics.oy_map_uc_odoo_top_40 as 
with base as
(
select 		distinct l.No , l.odoo_id , l.Customer_Name
			--, l.charging_id_manual, uc.charging_id as uc_charging_id
			, COALESCE(l.charging_id_manual , uc.charging_id) as charging_id
from 		(
				select No 	, "Odoo ID"as odoo_id , "Customer Name" as Customer_Name 
							, NULL::integer as charging_id_manual
				from 		sandbox.oy_sales_plans_gsheet
				union 
				select * from sandbox.oy_map_odoo_uc_obeid order by No
			) as l
left join 	(select distinct odoo_id, pd_user_id from standard.map_company_extended) as mp
on 			l.odoo_id = mp.odoo_id 
			and l.No not in (2,3) -- Zain is an exception and pd_user_id_manual needs to be input
left join	(select distinct provisioning_id, charging_id from raw.unifonic_cloud__account) as uc
on			uc.provisioning_id = mp.pd_user_id
order by 	l.No
)
select 		l.*
			, case when r.charging_id_manual is not null then 1 else null end as Obeid
from 		base as l
left join	sandbox.oy_map_odoo_uc_obeid as r
on 			l.charging_id = r.charging_id_manual
order by 	l.No
;