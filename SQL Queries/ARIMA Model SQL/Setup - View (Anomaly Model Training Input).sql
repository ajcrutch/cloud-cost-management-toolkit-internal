-- TODO: ideally would parameterize: timezone, net cost definition, minimum annual project spend

-- Notes:
-- 1) Only need to rebuild these tables at the frequency of retraining the ARIMA model. Typically this is monthly since project spending trends do not change much faster than monthly. Often done on the second day of the month, though there is no monthly aggregations underlying this analysis so the actual day of month is irrelevant

-- THIS IS HAPPENING IN DATAFORM NOW
-- Create a summary of the interesting metric (net cost) on the time grain of interest (daily usage) split out by a actionable category (project)
-- create or replace table `<project_id>`.`<dataset>`.`project_daily_spend` as --would rather this be a table or matview so that it can be used in other downstream queries efficiently
-- select project_id, project_name, usage_start_date, sum(total_cost)+sum(total_fcud_legacy) as total_net_cost --credits are negative
-- from `<project_id>`.`<dataset>`.`gcp_billing_export_daily_summary`
-- where usage_start_date >= DATE_SUB(CURRENT_DATE('US/Pacific'), INTERVAL 400 DAY)
-- and project_id is not null
-- group by 1,2,3;
-- Create actual training data set for ARIMA model
create or replace view `<project_id>`.`<dataset>`.`vw_project_input_data_net_cost` as (
-- Exclude projects with insufficient spend from any machine learning models
with project_lifetime_spend as (
select project_id, project_name, sum(total_net_cost) as total_net_cost_past_365_days
from `<project_id>`.`<dataset>`.`project_daily_spend`
where date_diff(current_date('US/Pacific'), usage_start_date, day) <= 365
group by 1,2
having total_net_cost_past_365_days>1000
) 
select daily.usage_start_date, daily.project_id, daily.project_name, sum(daily.total_net_cost) as total_net_cost
from `<project_id>`.`<dataset>`.`project_daily_spend` daily
inner join project_lifetime_spend lifetime on daily.project_id = lifetime.project_id -- inner joining with lifetime spend table filters away projects with insufficient spend
where date_diff(current_date('US/Pacific'), daily.usage_start_date, day) <= 400
and usage_start_date <= date_sub(current_date('US/Pacific'), INTERVAL 2 DAY) --ensure best we can that data is from complete days
and daily.project_id IS NOT NULL
group by 1,2,3
);