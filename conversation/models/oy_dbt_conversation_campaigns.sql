select 		    fc.*
                , lm.Master_Account_Name
                , lm.Master_Account_ID
                , da.pd_user_user_name 
                , da.pd_user_account_name
from 		    {{ source('aggregate', 'fact_campaigns') }} as fc
----------------
left join	    {{ source('standard', 'dim_accounts') }} as da
on 			    fc.account_id = da.account_nk
----------------
left join 	    {{ source('analytics', 'lm_dbt_master_account_mapping') }} as lm
on 			    fc.account_id =lm.UC_Account_ID_L0 
