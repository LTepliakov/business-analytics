with tm as 
(
select 		    ROW_NUMBER() over (partition by odoo_id order by hs_team) as rn 
                , odoo_id, hs_team, hs_manager, tcsm_manager
from 		    {{ source('analytics', 'oy_v_odoo_hs_team_manager') }}
where 		    hs_team is not null and hs_manager is not null
order by 	    hs_manager
)
select          odoo_id, hs_team, hs_manager, tcsm_manager
from            tm
where           rn = 1