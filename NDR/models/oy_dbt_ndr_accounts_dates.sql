with dates as
(
select 		distinct First_date, Year, Month 
from 		{{ ref('oy_dbt_date_range')}} -- verticadb"."analytics"."oy_dbt_date_range
),
accounts as
(
select 		distinct Client_ID, Client_Account_Name as Client_Name, business_domain
			, 2021 as '2021', 2022 as '2022', 2023 as '2023', 2024 as '2024'
from 		{{ source('analytics', 'oy_v_GL_revenue_all_domains')}} -- verticadb"."analytics.oy_v_GL_revenue_all_domains
where       Client_ID is not null
)
select 		First_date, dt.Year::VARCHAR , Month
			, COALESCE (ac1.Client_ID, ac2.Client_ID, ac3.Client_ID, ac4.Client_ID) as Client_ID
			, COALESCE (ac1.Client_Name, ac2.Client_Name, ac3.Client_Name, ac4.Client_Name) as Client_Name
			, COALESCE (ac1.business_domain, ac2.business_domain, ac3.business_domain, ac4.business_domain) as business_domain
from		dates as dt
------------
left join 	accounts as ac1
on 			dt.year = ac1.'2021'
------------
left join 	accounts as ac2
on 			dt.year = ac2.'2022'
------------
left join 	accounts as ac3
on 			dt.year = ac3.'2023'
------------
left join 	accounts as ac4
on 			dt.year = ac4.'2024'
------------
where 		First_date < now() - day(now())
order by 	Client_Name, dt.First_date