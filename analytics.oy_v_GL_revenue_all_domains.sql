drop view if exists analytics.oy_v_GL_revenue_all_domains
;
create view analytics.oy_v_GL_revenue_all_domains as
select 		TO_CHAR(Posting_Date, 'YYYY') as year
			, TO_CHAR(Posting_Date, 'Mon') as month
			, LAST_DAY(ADD_MONTHS(Posting_Date, -1)) + 1 as first_date
			, Client_ID
			, Client_Account_Name
			, business_domain
			, sum(Revenue_USD) as Revenue_USD
from 		analytics.oy_v_GL_profit_loss
where 		1=1
			--AND Revenue_USD > 0
			AND GL_Accounting_Name_Group = 'income'
group by	1,2,3,4,5,6
;