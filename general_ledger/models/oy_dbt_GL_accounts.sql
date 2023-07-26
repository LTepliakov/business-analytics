
select 			distinct 
                aa.code,
				gl.id as GL_ID,
				gl.date as Posting_Date,
				gl.ref as GL_Reference,
				gl.parent_state as GL_Status,
				gl.company_id as UF_Company_ID,
				gl.quantity as Quantity,
				gl.price_unit as Unit_Price,
				gl.discount as Discount,
				gl.debit as Debit_Local,
				gl.credit as Credit_Local,
				gl.partner_id as Odoo_ID,
				gl.product_id as Product_ID,
				gl.create_date as GL_Create_Date,
				gl.aggr_balance_report as Revenue_USD,
				gl.product_category_id as Product_Category_ID,
				rp.name as Client_Account_Name,
				rp.create_date as Client_Account_Create_Date,
				rp.ref as CRM_Company_ID,
				rp.vat as Client_Account_VAT_ID,
				rp.website as Client_Account_Website,
				rp.active as Client_Account_Active_Flag,
				rp.city as Client_Account_City,
				rp.email as Client_Account_Email,
				rp.mobile as Client_Account_Mobile,
				rp.legal_name as Client_Account_Legal_Name_AR,
				rp.cr as Client_Account_CR_ID,
				rp.parent_id as Client_Account_Parent_ID,
				rp2.name as Client_Account_Parent_Name,
				aa.name as GL_Accounting_Name,
				aa.internal_group as GL_Accounting_Name_Group,
				c.name as Currency_Code,
				rc.name as Unifonic_Entity,
				co.name as Client_Account_Country	
from  			{{ source('raw', 'odoo__account_move_line') }} as gl
left join 		{{ source('raw', 'odoo__res_partner') }} as rp ON gl.partner_id =rp.id 
left join 		{{ source('raw', 'odoo__res_partner') }} as rp2 ON rp.parent_id =rp2.id 
left join 		{{ source('raw', 'odoo__account_account') }} as aa on gl.account_id =aa.id
left join 		{{ source('raw', 'odoo__res_currency') }} as c on gl.currency_id =c.id
left join 		{{ source('raw', 'odoo__res_company') }} as rc on rc.id=gl.company_id 
left join 		{{ source('raw', 'odoo__res_country') }} as co on co.id =gl.partner_country_id 
where 			aa.code in (
                            '514003'
                            ,'420004','420005'
                            ,'410001','410002','410003','410004','410005'
                            ,'511001','511002','511003','511004','511005','511007','511008'
                            ,'512001','512002','512003'
                            ,'513002','513003'
                            ,'514001','514002'
                            ,'515001','515002')
				and gl.parent_state='posted'