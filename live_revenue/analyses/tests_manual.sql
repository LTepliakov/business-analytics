--SANDBOX
select * from sandbox.oy_dbt_ocs_balance
;
select * from sandbox.oy_dbt_ranking_sell_rates
;
select * from sandbox.oy_dbt_selling_rates_imputed
;
select * from sandbox.oy_dbt_revenue_daily_enriched
;

--ANALYTICS
select * from analytics.oy_ocs__balance_1
;
select * from analytics.oy_ranking_sell_rates
;
select * from analytics.oy_v_selling_rates_imputed
;
select * from analytics.oy_revenue_daily_enriched
;
select * from analytics.oy_dbt_revenue_daily_enriched
;

--TESTS
select * from analytics.oy_dbt_ocs_balance
except
select * from analytics.oy_ocs__balance_1
;
select * from analytics.oy_dbt_ranking_sell_rates
except
select * from analytics.oy_ranking_sell_rates
;
select * from analytics.oy_dbt_selling_rates_imputed
except
select * from analytics.oy_v_selling_rates_imputed
;
select * from analytics.oy_dbt_sales_plans_gsheet
except
select * from sandbox.oy_sales_plans_gsheet order by No
;
select * from analytics.oy_dbt_revenue_daily_enriched --order by date_nk  desc
except
select * from analytics.oy_revenue_daily_enriched --order by date_nk desc
;

--DROP ANALYTICS
drop table if exists analytics.oy_dbt_ocs_balance;
drop table if exists analytics.oy_dbt_ranking_sell_rates;
drop table if exists analytics.oy_dbt_revenue_daily_enriched;
drop table if exists analytics.oy_dbt_selling_rates_imputed;
drop table if exists analytics.example;
drop view if exists analytics.oy_dbt_dim_accounts;

--DROP SANDBOX
drop table if exists sandbox.oy_dbt_ocs_balance;
drop table if exists sandbox.oy_dbt_ranking_sell_rates;
drop table if exists sandbox.oy_dbt_selling_rates_imputed;
drop table if exists sandbox.oy_dbt_revenue_daily_enriched;
drop view if exists sandbox.oy_dbt_ocs_balance;
drop view if exists sandbox.oy_dbt_ranking_sell_rates;

--SEED TABLE
select * from analytics.oy_special_accounts
;
--##########################################################################################

select max() from aggregate.fact_sms_consumption_aggregate


