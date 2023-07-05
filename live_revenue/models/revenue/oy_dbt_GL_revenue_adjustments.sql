{{ config(materialized='ephemeral')}}

select 		first_date, Client_ID , Client_Account_Name, sum(Revenue_USD) as GL_Revenue_adjustm
from 		{{ source('analytics', 'oy_v_GL_revenue_reference')}} --analytics.oy_v_GL_revenue_reference --analytics.oy_v_GL_revenue_all_domains replaced
where 		1=1 
            and year >= {{var('GL_start_year')}}
            and business_domain = 'SMS'
            and Client_ID is not NULL 
            and GL_Reference ilike '%Adjustments%'
            --and Client_ID=3368
            group by 	1,2,3
order by 	Client_ID , first_date desc