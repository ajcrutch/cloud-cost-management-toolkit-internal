/*
This file contains the one-time setup for creating the necessary
tables that the alerting pipeline relies on.
*/

-- ============================================================================
-- 1. Create Anomaly Alert Log Table
-- ============================================================================
--Must first drop if exists in order to allow altering partitioning and/or clustering method
drop table if exists `<project_id>.<dataset>.cost_anomaly_alert_log`;

CREATE OR REPLACE TABLE `<project_id>.<dataset>.cost_anomaly_alert_log` (
  alert_log_timestamp TIMESTAMP,
  project_id STRING,
  usage_start_date DATE,
  owner STRING,
  slack_recipient_list STRING,
  email_alert_success BOOL,
  slack_alert_success BOOL,
  alert_suppressed BOOL,
  expected_spend NUMERIC,
  actual_cost NUMERIC,
  absolute_distance_from_threshold NUMERIC,
  absolute_percent_from_threshold NUMERIC,
  anomaly_id STRING,
  most_likely_service STRING,
  ai_recommendation STRING
)
PARTITION BY
  -- Changed to usage_start_date as it's the primary query filter
  usage_start_date
CLUSTER BY
  project_id, anomaly_id
OPTIONS (
  description = 'Logs all cost anomaly alerts sent and suppressed.'
);
