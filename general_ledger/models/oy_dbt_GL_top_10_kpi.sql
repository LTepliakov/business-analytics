-- replacement for
-- https://tableau.data.ksa-1.oci.cloud.unifonic.com/#/datasources/1947?:origin=card_share_link
with tm as 
(
select 		distinct
            odoo_id, odoo_name, email, hs_team, hs_manager, hs_email
from 		{{ source('analytics', 'oy_v_odoo_hs_team_manager') }}
where 		hs_team is not null and hs_manager is not null
order by 	hs_manager
)
select 			distinct 
                gl.*
                , tm.hs_team
                , tm.hs_manager
from  			{{ ref('oy_dbt_GL_accounts')}} as gl
left join 		tm
on 				gl.Odoo_ID = tm.odoo_id