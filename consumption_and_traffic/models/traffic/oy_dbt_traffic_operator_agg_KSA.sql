{{config (materialized='view')}}

with base as
(
select          *
                , case when     (
                                network_name ilike '%Zain%'
                                or network_name ilike '%Mobily%'
                                or network_name ilike '%Lebara%'
                                or network_name ilike '%Virgin%'
                                )
                                --and country_id = 3 -- speak to Dayan see below why
                                then 'KSA_only' -- ask Dayana about the super rare use cases for these operator whne country_id <> 3. Why?
                       when     network_name ilike '%STC%' then 'KSA_and_International' -- if country_id is essential for alledgedly KSA networks only then I need ti uncomment country_id = 3 clause for them
                       when     network_name is null then 'Unknown'
                       else     'International'
                       end as network_is
from            {{ ref('oy_dbt_traffic_operator_agg')}}
)
, cats as
(
select          *
                , case when     network_is = 'KSA_only' then 'KSA' -- ask Dayana about the super rare use cases for these operator whne country_id <> 3. Why?
                       when     network_is = 'KSA_and_International' and country_id = 3 then 'KSA'
                       when     network_is = 'Unknown' then 'Unknown'
                       else 'International'
                       end as traffic_is
                , case when cost_id is null then 'Unknown'
                       else 'Known'
                       end as cost_id_is
from            base
)
select 		    *
from 		    cats
----------------
where		    1=1
                and network_name not ilike 'Internal%'
                and traffic_is = 'KSA'
