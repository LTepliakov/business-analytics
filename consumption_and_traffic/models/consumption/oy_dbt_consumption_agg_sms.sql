{{config (materialized='table')}} 

select          *
                , case when     network_name ilike '%STC%'
                                or network_name ilike '%Zain%'
                                or network_name ilike '%Mobily%'
                                or network_name ilike '%Lebara%'
                                or network_name ilike '%Virgin%'
                       then 'KSA'
                       else 'International'
                  end as traffic_is
from            {{ ref('oy_dbt_consumption_agg')}} as ca
where           1=1
                and     (
                        ca.event_type = 'default.sms' 
                        or ca.general_ledger_code='Forfeiture Of Package Balance'
                        )
                and date_nk >= '2023-01-01'
                --and Account_Type in ('Client Account - Open', 'Client Account')