
drop view if exists analytics.lm_v_master_account_mapping
;
create view analytics.lm_v_master_account_mapping as

with base as
(
SELECT 			a1.account_nk AS UC_Account_ID_L0,
		        a1.pd_user_account_name AS UC_Account_Name_L0,
		        u1.user_email AS UC_UserName_L0,
		        a1.pd_user_country as UC_Account_Country_L0,
		        a1.charging_id AS Charging_ID_L0,
		        a1.pd_user_id as PD_User_ID_L0,
		        a1.customer_id as Customer_ID_L0,
		        p1.id AS Odoo_ID_L0,
		        p1.name AS Odoo_Account_Name_L0,
		        
		        a2.account_nk AS "UC_Account_ID_L+1",
		        a2.pd_user_account_name AS "Account_Name_L+1",
		        u2.user_email  AS "UC_UserName_L+1",
		        a2.charging_id AS "Charging_ID_L+1",
		        a2.customer_id as "Customer_ID_L+1",
		        p2.id AS "Odoo_ID_L+1",
		        p2.name AS "Odoo_Account_Name_L+1",
		        
		        a3.account_nk AS "UC_Account_ID_L+2",
		        a3.pd_user_account_name AS "UC_Account_Name_L+2",
		        u3.user_email  AS "UC_UserName_L+2",
		        a3.charging_id AS "Charging_ID_L+2",
		        a3.customer_id as "Customer_ID_L+2",
		        p3.id AS "Odoo_ID_L+2",
		        p3.name AS "Odoo_Account_Name_L+2",
		        
		        a4.account_nk AS "UC_Account_ID_L+3",
		        a4.pd_user_account_name AS "UC_Account_Name_L+3",
		        u4.user_email  AS "UC_UserName_L+3",
		        a4.charging_id AS "Charging_ID_L+3",
		        a4.customer_id as "Customer_ID_L+3",
		        p4.id AS "Odoo_ID_L+3",
		        p4.name AS "Odoo_Account_Name_L+3",
		        coalesce((p4.id)::varchar, (p3.id)::varchar, (p2.id)::varchar, (p1.id)::varchar, a4.account_nk, a3.account_nk, a2.account_nk, a1.account_nk) AS Master_Account_ID,
		        coalesce(p4.name, p3.name, p2.name, p1.name, a4.pd_user_account_name, a3.pd_user_account_name, a2.pd_user_account_name, a1.pd_user_account_name) AS Master_Account_Name,
		        CASE 	WHEN (p4.id IS NOT NULL) THEN 'Odoo_L+3'::varchar(8) 
		        		WHEN ((p4.id IS NULL) AND (p3.id IS NOT NULL)) THEN 'Odoo_L+2'::varchar(8) 
		        		WHEN ((p4.id IS NULL) AND (p3.id IS NULL) AND (p2.id IS NOT NULL)) THEN 'Odoo_L+1'::varchar(8) 
		        		WHEN ((p4.id IS NULL) AND (p3.id IS NULL) AND (p2.id IS NULL) AND (p1.id IS NOT NULL)) THEN 'Odoo_L0'::varchar(7) 
		        		WHEN ((p4.id IS NULL) AND (p3.id IS NULL) AND (p2.id IS NULL) AND (p1.id IS NULL) AND (a4.account_nk IS NOT NULL)) THEN 'UC_L+3'::varchar(6) 
		        		WHEN ((p4.id IS NULL) AND (p3.id IS NULL) AND (p2.id IS NULL) AND (p1.id IS NULL) AND (a4.account_nk IS NULL) AND (a3.account_nk IS NOT NULL)) THEN 'UC_L+2'::varchar(6) 
		        		WHEN ((p4.id IS NULL) AND (p3.id IS NULL) AND (p2.id IS NULL) AND (p1.id IS NULL) AND (a4.account_nk IS NULL) AND (a3.account_nk IS NULL) AND (a2.account_nk IS NOT NULL)) THEN 'UC_L+1'::varchar(6) 
		        		WHEN ((p4.id IS NULL) AND (p3.id IS NULL) AND (p2.id IS NULL) AND (p1.id IS NULL) AND (a4.account_nk IS NULL) AND (a3.account_nk IS NULL) AND (a2.account_nk IS NULL) AND (a1.account_nk IS NOT NULL)) THEN 'UC_L0'::varchar(5) 
		        		ELSE NULL 
		        END AS Master_Account_Origin,
                
		        coalesce(a4.customer_id,a3.customer_id,a2.customer_id,a1.customer_id) AS Master_Customer_ID,
		        
                coalesce(a4.account_nk,a3.account_nk,a2.account_nk,a1.account_nk) as UC_Super_Parent_ID,
                coalesce(a4.pd_user_account_name,a3.pd_user_account_name,a2.pd_user_account_name,a1.pd_user_account_name) as UC_Super_Parent_Name,
                coalesce(u4.user_email,u3.user_email,u2.user_email,u1.user_email) as UC_Super_Parent_Username,
                
                case 	when a2.account_nk is null then 'Main Account' else 'Sub-Account' end as Main_Sub_Flag 
                
 FROM 			
                standard.dim_accounts as a1 
                JOIN standard.dim_users u1 on a1.owner_id=u1.user_nk 
                LEFT JOIN 	raw.odoo__res_partner as p1 ON ((p1.id = a1.erp_id))
                
                LEFT JOIN 	standard.dim_accounts as a2 ON ((a1.parent_account_nk = a2.account_nk))
                LEFT JOIN   standard.dim_users u2 on a2.owner_id=u2.user_nk 
                LEFT JOIN 	raw.odoo__res_partner as p2 ON ((p2.id = a2.erp_id))
                 
                LEFT JOIN 	standard.dim_accounts as a3 ON ((a2.parent_account_nk = a3.account_nk))
                LEFT JOIN   standard.dim_users u3 on a3.owner_id=u3.user_nk 
                LEFT JOIN 	raw.odoo__res_partner as p3 ON ((p3.id = a3.erp_id))
                 
                LEFT JOIN 	standard.dim_accounts as a4 ON ((a3.parent_account_nk = a4.account_nk))
                LEFT JOIN   standard.dim_users u4 on a4.owner_id=u4.user_nk 
                LEFT JOIN 	raw.odoo__res_partner as p4 ON ((p4.id = a4.erp_id))
                
 )
 select 		*
				, case when Charging_ID_L0 in ('20005065', '20004825', '20004828', '10004615') then '10626'
					   when Charging_ID_L0 in ('10005927') then '3762'
					   when Charging_ID_L0 in ('10005710', '10005891', '20004828', '20000163') then '4112'
					   when Charging_ID_L0 in ('20001516') then '3168'
					   when Charging_ID_L0 in ('10000326') then '4154'
					   when Charging_ID_L0 in ('20000477', '10006034') then '3525'
					   when Charging_ID_L0 in ('10000297') then '3374'
					   when Charging_ID_L0 in ('20003684', '20005029') then '10857'
					   when Charging_ID_L0 in ('20002128') then '3704'
					   when Charging_ID_L0 in ('20000934') then '5147'
					   when Charging_ID_L0 in ('10002291') then '4358'
					   when Charging_ID_L0 in ('10002202') then '3368'
					   else null 
				  end as manual_odoo_id
				, case when Charging_ID_L0 in ('20005065', '20004825', '20004828', '10004615') then 'Careem Networks FZ LLC'
				 	   when Charging_ID_L0 in ('10005927') then 'Tatweer Educational Technologies Company - TETCO'
					   when Charging_ID_L0 in ('10005710', '10005891', '20004828', '20000163') then 'Saudi Manpower Solutions Co.'
					   when Charging_ID_L0 in ('20001516') then 'Azim Technical Financial Company Ltd.'
					   when Charging_ID_L0 in ('10000326') then 'Takamol Holding'
					   when Charging_ID_L0 in ('20000477', '10006034') then 'Al-Romansiah Resturant Co .Ltd'
					   when Charging_ID_L0 in ('10000297') then 'Rawabi Majd International Company for Production and Distribution Ltd. - Al-Majd TV'
					   when Charging_ID_L0 in ('20003684', '20005029') then 'Food Basics Trading Company - Hamburgini'
					   when Charging_ID_L0 in ('20002128') then 'Aram Meem Trading Services Company - Toyou'
					   when Charging_ID_L0 in ('20000934') then 'Mobile Telecommunications Company - Zain Saudi Arabia'
					   when Charging_ID_L0 in ('114') then 'Al-Nahdi Medical Company'
					   when Charging_ID_L0 in ('10002291') then 'Hamad M AlRugaib and Sons Trading Co. - Al-Rugaib Furniture'
					   when Charging_ID_L0 in ('10002202') then 'Nakhla Information Systems Technology'
					   else null 
				  end as manual_odoo_name
 from  			base
 
 
