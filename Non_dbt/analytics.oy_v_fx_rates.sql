create view analytics.oy_v_fx_rates as
select 			c.name as date_nk
				, c.rate
				, n.name as target_currency
				, n1.name as base_currency
				, c.currency_id 
				, c.company_id 
				, c.create_date 
from 			raw.odoo__res_currency_rate as c
left join 		raw.odoo__res_currency as n
on 				c.currency_id = n.id 
left join 		raw.odoo__res_company as r
on 				c.company_id = r.id
left join 		raw.odoo__res_currency as n1
on 				r.currency_id = n1.id
where 			n1.name = 'USD'
order by 		date_nk desc, target_currency 
;
select * from analytics.oy_v_fx_rates
order by date_nk desc, target_currency 