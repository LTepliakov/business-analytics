with reccuring_21_22 as
(
select 		Client_ID, Client_Account_Name , Month , business_domain 
			, sum(YTD_flag) as Reccuring_rev_2022
from 		{{ ref('oy_dbt_ndr_ytd_revenue')}}
where 		Year in ('2021', '2022')
group by 	1,2,3,4
order by 	Client_ID, Month
)
select 		*
			, case when  Reccuring_rev_2022 > 1 then 1 else 0 end as NDR_22
from 		reccuring_21_22