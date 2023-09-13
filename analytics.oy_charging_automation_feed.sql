drop view if exists analytics.oy_charging_automation_feed
;
CREATE VIEW analytics.oy_charging_automation_feed AS
SELECT 		CASE WHEN pr.UC_Account_ID_L0 IS NULL THEN NULL ELSE 'main'::varchar(4) END AS acc_type,
	        al.UC_Account_ID_L0,
	        CASE WHEN length(al.Master_Account_ID) <= 5 THEN al.Master_Account_ID ELSE NULL END AS odoo_id,
	        tm.id AS hs_id,
	        al.Charging_ID_L0,
	        al.UC_UserName_L0,
	        al.UC_Account_Name_L0,
	        al.Master_Account_Name,
	        uc.customer_id
FROM 		analytics.lm_v_master_account_mapping as al
LEFT JOIN 	(
			SELECT 		UC_Account_ID_L0,
				        "UC_Account_ID_L+1"
			FROM 		analytics.lm_v_master_account_mapping
			WHERE 		"UC_Account_ID_L+1" IS NULL
			) as pr
ON 			al.UC_Account_ID_L0 = pr.UC_Account_ID_L0
------------
LEFT JOIN 	(
			SELECT 		hubspot__companies.odoo_id,
        				hubspot__companies.id
			FROM 		raw.hubspot__companies
			GROUP BY 	1,2
			) as tm
ON 			al.Master_Account_ID = tm.odoo_id
------------
LEFT JOIN 	raw.unifonic_cloud__account as uc
ON 			al.UC_Account_ID_L0 = uc.id
------------
ORDER BY 	al.Odoo_ID_L0
;
select * from analytics.oy_charging_automation_feed
;