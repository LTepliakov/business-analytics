{{config (materialized='view')}}

select 		    *
from 		    {{ ref('oy_dbt_traffic_operator_agg') }}
----------------
where		    1=1
                and network_name not ilike 'Internal%'
                and destination_country = 'KSA'
                and (
                        network_name ilike '%STC%'
                        or network_name ilike '%Zain%'
                        or network_name ilike '%Mobily%'
                        or network_name ilike '%Lebara%'
                        or network_name ilike '%Virgin%'
                    )
