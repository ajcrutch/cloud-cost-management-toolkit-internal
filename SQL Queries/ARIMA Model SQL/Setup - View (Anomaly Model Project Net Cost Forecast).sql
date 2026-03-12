-- CREATE OR REPLACE VIEW `<project_id>.<dataset>.vw_project_net_cost_forecast`  as (

-- with latest_model_info as (
--   select max_usage_start_date
--   from `<project_id>.<dataset>.ANOMALY_DETECTION_BQML_MODEL_INFO` 
--   WHERE model_name = 'project_net_cost_forecast'
--   AND is_latest_model_version = TRUE
-- )

-- select coalesce(daily.project_id,forecast.project_id) project_id, coalesce(daily.total_net_cost,forecast.net_cost) as net_cost_combined, daily.total_net_cost as actual_net_cost, forecast.net_cost as forecast_net_cost
-- from `<project_id>.<dataset>.vw_gcp_billing_export_daily_summary` daily
-- left join `<project_id>.<dataset>.project_forecasts` forecast 
--   on daily.project_id = forecast.project_id 
--   and daily.usage_start_date = forecast.usage_start_date
-- cross join latest_model_info
-- where daily.usage_start_date < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
-- and forecast.usage_start_date >= latest_model_info.max_usage_start_date
-- );

CREATE OR REPLACE VIEW `<project_id>.<dataset>.vw_project_net_cost_forecast`  as (
with latest_model_info as (
  select max_usage_start_date
  from `<project_id>.<dataset>.ANOMALY_DETECTION_BQML_MODEL_INFO` 
  WHERE model_name = 'project_net_cost_forecast'
  AND is_latest_model_version = TRUE
)
SELECT
    'Actual' as actuals_or_forecast,
    date(usage_start_date) as usage_start_date,
    project_id,
    total_net_cost AS net_cost,
    NULL AS forecasted_net_cost,
    NULL AS prediction_interval_lower_bound,
    NULL AS prediction_interval_upper_bound
  FROM
    ML.DETECT_ANOMALIES( -- Calling this on the model w/o new data returns the original training data + anomaly columns
      MODEL `<project_id>.<dataset>.project_net_cost_forecast`
    ) 
  UNION ALL

  -- Part 2: Future Forecasts
  SELECT
    'Forecast' as actuals_or_forecast,
    date(forecast_timestamp) AS usage_start_date,
    project_id,
    NULL AS net_cost,
    forecast_value AS forecasted_net_cost,
    prediction_interval_lower_bound,
    prediction_interval_upper_bound
  FROM
    ML.FORECAST(
      MODEL `<project_id>.<dataset>.project_net_cost_forecast`,
      STRUCT(30 AS horizon, 0.95 AS confidence_level)
    ) as forecast
  cross join latest_model_info 
  where date(forecast.forecast_timestamp) > latest_model_info.max_usage_start_date --necessary because ARIMA forecasts are created whenever projects stop spending, making a lot of "forecast" data in the past due to projects which stopped being active in the past
);


--For future dev, it might make a difference to recalculate the forecast to alter the upper and lower bound to reflect the effects of aggregating error bands. This example assumes model trained to output 
-- AggregatedForecast AS (
--   SELECT
--     usage_start_date,
--     SUM(forecast_value) AS total_forecasted_cost,
--     -- The standard error of a sum of independent variables is the
--     -- square root of the sum of their variances.
--     SQRT(SUM(forecast_variance)) AS total_standard_error
--   FROM
--     ProjectForecasts
--   GROUP BY
--     usage_start_date
-- )
-- -- Re-calculate the 95% confidence interval for the aggregated total
-- -- (Z-score for 95% CI is approx 1.96)
-- SELECT
--   usage_start_date,
--   total_forecasted_cost,
--   total_standard_error,
--   (total_forecasted_cost - 1.96 * total_standard_error) AS total_prediction_interval_lower_bound,
--   (total_forecasted_cost + 1.96 * total_standard_error) AS total_prediction_interval_upper_bound
-- FROM
--   AggregatedForecast
-- ORDER BY
--   usage_start_date;