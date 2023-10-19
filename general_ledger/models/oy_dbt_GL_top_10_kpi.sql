-- replacement for
-- https://tableau.data.ksa-1.oci.cloud.unifonic.com/#/datasources/1947?:origin=card_share_link
select 			distinct
                TO_CHAR(Posting_Date, 'YYYY') as Year
			    , TO_CHAR(Posting_Date, 'Mon') as month
			    , LAST_DAY(ADD_MONTHS(Posting_Date, -1)) + 1 as First_date
                , gl.*
                , hs_team
                , hs_manager
from  			{{ ref('oy_dbt_GL_accounts')}} as gl
left join 		{{ ref('oy_dbt_odoo_hs_team_manager_distinct') }} as tm
on 				gl.Odoo_ID = tm.odoo_id