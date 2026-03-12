/*
This file creates the base view for detecting anomalies from your
ML.DETECT_ANOMALIES model.
*/

-- ============================================================================
-- Create Anomaly Detection View
-- ============================================================================
CREATE OR REPLACE VIEW `<project_id>.<dataset>.vw_project_detect_anomalies_net_cost` AS
WITH
  anomaly_data_1 AS (
    SELECT
      c.usage_start_date AS usage_start_date,
      c.project_name AS project_name,
      c.project_id AS project_id,
      c.total_net_cost AS total_net_cost,
      coalesce(c.is_anomaly, h.is_anomaly) AS is_anomaly,
      coalesce(c.lower_bound, h.lower_bound) AS lower_bound,
      coalesce(c.upper_bound, h.upper_bound) AS upper_bound,
      coalesce(c.anomaly_probability, h.anomaly_probability) AS anomaly_probability
    FROM
      (
        --Forecasted input_data (returns all rows but only anomaly details for forecasting period)
        SELECT
          *
        FROM
          ML.DETECT_ANOMALIES(
            MODEL `<project_id>.<dataset>.project_net_cost_forecast`,
            STRUCT(CAST(0.99 AS NUMERIC) AS anomaly_prob_threshold),
            (
              SELECT
                *
              FROM
                `<project_id>`.`<dataset>`.`project_daily_spend`
            )
          )
      ) c
      LEFT JOIN (
        --Modeled input_data (returns only rows and anomaly details for historical data used to create model)
        SELECT
          *
        FROM
          ML.DETECT_ANOMALIES(
            MODEL `<project_id>.<dataset>.project_net_cost_forecast`,
            STRUCT(CAST(0.99 AS NUMERIC) AS anomaly_prob_threshold)
          )
      ) h ON c.project_id = h.project_id
      AND c.usage_start_date = h.usage_start_date
  ),
  anomaly_data_2 AS (
    SELECT
      *,
      CASE
        WHEN total_net_cost > upper_bound THEN total_net_cost - upper_bound
        ELSE NULL
      END AS delta_above_upper_bound,
      CASE
        WHEN total_net_cost < lower_bound THEN total_net_cost - lower_bound
        ELSE NULL
      END AS delta_below_lower_bound,
      CASE
        WHEN total_net_cost > upper_bound THEN (SAFE_DIVIDE(total_net_cost - upper_bound, upper_bound))
        ELSE NULL
      END AS percent_above_upper_bound,
      CASE
        WHEN total_net_cost < lower_bound THEN (SAFE_DIVIDE(total_net_cost - lower_bound, lower_bound))
        ELSE NULL
      END AS percent_below_lower_bound
    FROM
      anomaly_data_1
  ),
  anomaly_data_3 AS (
    SELECT
      *,
      CASE
        WHEN is_anomaly THEN coalesce(delta_above_upper_bound, delta_below_lower_bound)
      END AS absolute_distance_from_threshold,
      abs(
        CASE
          WHEN is_anomaly THEN coalesce(percent_above_upper_bound, percent_below_lower_bound)
        END
      ) AS absolute_percent_from_threshold
    FROM
      anomaly_data_2
  )
SELECT
  *,
  -- Add the anomaly_direction field here
  CASE
    WHEN is_anomaly
    AND total_net_cost > upper_bound THEN 'Above'
    WHEN is_anomaly
    AND total_net_cost < lower_bound THEN 'Below'
    ELSE NULL
  END AS anomaly_direction
FROM
  anomaly_data_3;

--   CREATE OR REPLACE VIEW `<project_id>.<dataset>.vw_project_explain_forecast_detect_anomalies_net_cost_join` as 
-- SELECT
--   * EXCEPT(project_id),
--   explain_forecast.project_id,
--   'owner' as owner --need to replace with join to owner table(s)
-- FROM
--   `<project_id>.<dataset>.vw_project_explain_forecast_net_cost` explain_forecast
-- LEFT JOIN
--   `<project_id>.<dataset>.vw_project_detect_anomalies_net_cost` detect_anomalies
-- ON
--   explain_forecast.project_id = detect_anomalies.project_id
--   AND explain_forecast.time_series_timestamp = detect_anomalies.usage_start_date;
