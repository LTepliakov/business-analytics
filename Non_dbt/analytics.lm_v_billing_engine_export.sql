
drop view if exists analytics.lm_v_billing_engine_export

;

create view analytics.lm_v_billing_engine_export as




Select 
            date_trunc('day',hour_nk)::DATE as Date,
            account_id as Charging_ID,
            Master_Account_ID as ERP_ID,
            Master_Customer_ID,
            
            split_part(bundle_name,'_',2) as Bundle_Name,
            current_selling_rate_value as Selling_Rate,
            current_selling_rate_currency as Currency_ISO,
            exchange_rate as FX_Rate,
            
            tariff,
            country,
            event_type,
            result,
			action,
       
            case 
                when split_part(bundle_name,'_',2) ='mb' then 'Balance'
                when bundle_name is null then 'Zero_Rated'
                else 'Package'
                end
            as Transaction_Type,
           
            sum(
            case 
            when split_part(bundle_name,'_',2) ='mb' then units
            when bundle_name is null then units
            else charge
            end)
            as Chargeable_Units,
            
            
            sum(
            case 
            when split_part(bundle_name,'_',2) ='mb' then charge
            when bundle_name is null then 0
            else charge*current_selling_rate_value
            end)
            as USD_Revenue
            
          

from aggregate.fact_sms_consumption_aggregate
join analytics.lm_v_master_account_mapping  on aggregate.fact_sms_consumption_aggregate.account_id = analytics.lm_v_master_account_mapping.Charging_ID_L0 

where date(hour_nk)>='2023-06-01'
and event_type='default.sms'
and result='SUCCESS'
and action in ('DIRECT_DEBIT','CHARGE','REFUND')


group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
order by 1 desc







