Select 
            date_trunc('day',hour_nk)::DATE as Date, 
            action as Action,
            result as Result,
            country as Destination_Country,
            sender_name as Sender_Name,
            network_name as Network_Name,
            tariff as Tariff,
            o.ocs_name as Operator_Name,
            
            account_id as Charging_ID,
            UC_account_id_L0 as UUID,
            UC_account_name_L0 as UC_Account_Name,
            UC_Username_L0 as UC_UserName,
            UC_account_country_L0 as Account_Country,
            Master_Account_ID,
            Master_Account_Name,
            Master_Account_Origin,
            Main_Sub_Flag,
            
            
            split_part(event_type,'.',2) as Product,
                   
            case  
                when split_part(bundle_name,'_',2) ='mb' then 'Balance'
                when bundle_name is null then 'Zero_Rated'
                else 'Package'
                end
            as Package_Vs_Balance,
            
            split_part(bundle_name,'_',2) as Bundle_Name,
            
            sum(units) as Units_Sent,
            
            sum(message_count) as Messages_Sent,
            
            sum(charge) as Charge,
            
            sum(
            case 
            when split_part(bundle_name,'_',2) ='mb' then charge
            else 0
            end)
            as USD_Balance_Deduction,
            
            sum(
            case 
            when split_part(bundle_name,'_',2) !='mb' then charge
            else 0
            end)
            as Unit_Package_Deduction
          
          

from aggregate.fact_sms_consumption_aggregate c
join analytics.lm_v_master_account_mapping  ma on c.account_id = ma.Charging_ID_L0 
left join standard.dim_operators o on c.operator_id=o.mprocra_abl_operator_id 

where 
date(hour_nk)>=  current_date - interval '24 month' 




group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20