
select              c.*
                    , d.fs_ticket_id as fs_first_ticket_id
                    , d.count_onboarding_tickets
                    , d.custom_fields__cf_service781181
                    , d.created_at::DATE
                    , d.stats__closed_at::DATE as closed_at
                    , d.at_least_one_ticket_closed
                    , d.first_closing_date
                    , datediff(day, d.created_at, c.threshold_hit_at) as days_to_threshold_hit
                    , datediff(day, d.created_at, c.last_consumption_at) as days_to_last_consumption
from                {{ ref('oy_dbt_consumption_start_date_concize')}} as c
left join           {{ ref('oy_dbt_customer_onboarding_dedupped')}} as d
on                  c.Master_Account_ID = d.fs_odoo_id
                    and c.channel = d.channel 