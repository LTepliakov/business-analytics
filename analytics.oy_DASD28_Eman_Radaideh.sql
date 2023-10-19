drop view if exists analytics.oy_DASD28_Eman_Radaideh
;
create view analytics.oy_v_DASD28_Eman_Radaideh as
with t as
(
select 				submit_date,
					account_id as FCDR_Account_ID,
					source_protocol,
					message_status,
					message_type,
					sender_name,
					event_status,
					--region,
					--mnp_used,
					--normalized_status,
					dlr_status,
					--campaign_id,
					--correlation_id,
					product,
					--product_name,
					customer_status,
					customer_reason,
					operator_id,
					country_id,
					network_id,
					sum(message_count) as Number_Of_Messages,
					sum(number_of_units) as Number_Of_Units
from 				aggregate.fact_sms_aggregate
where 				date(submit_date) >= (select max(submit_date)-30 from aggregate.fact_sms_aggregate)
group by 			1,2,3,4,5,6,7,8,9,10,11,12,13,14--,15,16,17,18,19,20
)
select 				ma.Master_Account_ID, ma.Master_Account_Name,
					t.*,
					--fa.fcdr_account_nk,
					ma.UC_Account_ID_L0 as UC_UUID,
					ma.UC_Account_Name_L0 as UC_Account_Name,
					ma.UC_UserName_L0 as UC_UserName,
					ma.UC_Account_Country_L0 as UC_Account_Country,
					ma.Charging_ID_L0 as Charging_ID,
					--ma.Odoo_ID_L0 as Odoo_ID,
					--ma.Odoo_Account_Name_L0 as Odoo_Account_Name,
					ma.Master_Account_Origin,
					o.ocs_name as Operator_Name,
					c.country_name as Destination_Country,
					N.Network_Name,
					tm.hs_team, tm.hs_manager, tm.tcsm_manager
from 				t
--------------------
join 				standard.dim_fcdr_accounts as fa 
on 					t.FCDR_Account_ID = fa.fcdr_account_nk 
--------------------
join 				analytics.lm_v_master_account_mapping as ma 
on 					fa.account_nk = ma.UC_Account_ID_L0 
--------------------
left join 			standard.dim_operators as o 
on 					t.operator_id = o.mprocra_abl_operator_id
--------------------
left join 			standard.dim_countries as c 
on 					t.country_id = c.mprocra_abl_country_id
--------------------
left join   		(
						SELECT		network_id_prefix as Network_ID,
									max(provider_name_for_administrator) as Network_Name
						FROM 		raw.pd__pd_esmes
						WHERE 		provider_name_for_administrator is not null
						group by 	1
					) as N 
on 					N.Network_ID=t.network_id
-------
left join 			(
						select 		distinct
									odoo_id, hs_team, hs_manager, tcsm_manager
						from 		analytics.oy_v_odoo_hs_team_manager
						where 		hs_team is not null and hs_manager is not null
						order by 	hs_manager
					) as tm 
on 					tm.odoo_id = ma.Master_Account_ID
where 				1=1
					and ma.Master_Account_Name not in ('migration traffic', 'Unifonic Employees')
					and ma.UC_Account_Name_L0 <> 'Active monitoring'
--order by 			Master_Account_ID, charging_id, submit_date
;
select * from analytics.oy_DASD28_Eman_Radaideh