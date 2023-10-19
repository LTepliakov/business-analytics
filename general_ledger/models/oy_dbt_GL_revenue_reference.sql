
select 		TO_CHAR(Posting_Date, 'YYYY') as year
			, TO_CHAR(Posting_Date, 'Mon') as month
			, LAST_DAY(ADD_MONTHS(Posting_Date, -1)) + 1 as first_date
			, Client_ID
			, Client_Account_Name
			, business_domain
			, GL_Accounting_Name
			, GL_Reference
			, sum(Revenue_USD) as Revenue_USD
from 		{{ ref('oy_dbt_GL_top_10_kpi') }}
where 		1=1
			AND GL_Accounting_Name_Group = 'income'
group by	1,2,3,4,5,6,7,8