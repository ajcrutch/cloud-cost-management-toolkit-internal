/*
This file contains the one-time setup for creating the necessary
tables that the alerting pipeline relies on.
*/

-- ============================================================================
-- 2. Create Anomaly Threshold Settings Table
-- ============================================================================
--Must first drop if exists in order to allow altering partitioning and/or clustering method
drop table if exists `<project_id>.<dataset>.project_period_anomaly_threshold_settings`;

CREATE OR REPLACE TABLE `<project_id>.<dataset>.project_period_anomaly_threshold_settings` (
  project_id STRING,
  setting_start_date DATE,
  setting_end_date DATE, --null will make this setting function indefinitely, or until a more recent setting creation timestamp is detected for that project
  absolute_percent_threshold FLOAT64,
  absolute_delta_threshold FLOAT64,
  max_cost_threshold FLOAT64, --alerts suppressed unless spend is higher than this value
  setting_creation_timestamp TIMESTAMP
)
CLUSTER BY
  project_id, setting_start_date
OPTIONS (
  description = 'Stores custom anomaly alerting thresholds for specific projects and time periods.'
);
