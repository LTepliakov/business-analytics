{{config (materialized='table')}} 

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
from            {{ ref('oy_dbt_consumption_agg')}}
where           1=1
                and     (
                        event_type = 'default.sms' 
                        or general_ledger_code='Forfeiture Of Package Balance'
                        )
                and date_nk >= '2023-01-01'
                --and Account_Type in ('Client Account - Open', 'Client Account')
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
, charge_with_cats as
(
select          *
                , sum(units) over (partition by Master_Account_Name) as client_charge
                , case when traffic_is = 'Unknown' then 0 
                       else units
                       end as 'categorized_charge'
                , case when cost_id_is ='Unknown' then 0 
                	   else units
                       end as 'charge_with_cost_id'
from            cats
)
, totals as
(
select          *
                , sum(categorized_charge) over (partition by Master_Account_Name) as categorized_charge_agg
                , sum(charge_with_cost_id) over (partition by Master_Account_Name) as charge_with_cost_id_agg
from            charge_with_cats
)
select          *
                , case when client_charge = 0 then null else round(100*(categorized_charge_agg/client_charge),1) end as categorized_charge_pct
                , case when client_charge = 0 then null else round(100*(charge_with_cost_id_agg/client_charge),1) end as charge_with_cost_id_pct
from            totals