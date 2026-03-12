WITH
  -- 1. Get the most recent custom anomaly threshold settings for each project and date.
  project_period_anomaly_threshold_settings_ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY
          project_id,
          calendar_date
        ORDER BY
          setting_creation_time DESC
      ) AS rank
    FROM
      (
        WITH
          calendar AS (
            SELECT
              d AS calendar_date
            FROM
              UNNEST(GENERATE_DATE_ARRAY('2023-01-01', '2050-01-01', INTERVAL 1 DAY)) AS d
          )
        SELECT
          s.project_id,
          TIMESTAMP(s.setting_start_date) AS setting_start_date,
          TIMESTAMP(COALESCE(s.setting_end_date, DATE('2199-01-01'))) AS setting_end_date,
          s.absolute_percent_threshold,
          s.absolute_delta_threshold,
          TIMESTAMP(DATETIME_TRUNC(calendar.calendar_date, DAY)) AS calendar_date,
          s.setting_creation_timestamp AS setting_creation_time,
          s.max_cost_threshold
        FROM
          calendar
          LEFT JOIN `<project_id>.<dataset>.project_period_anomaly_threshold_settings` AS s ON DATE(calendar.calendar_date) BETWEEN s.setting_start_date
          AND COALESCE(s.setting_end_date, DATE(2199, 01, 01))
        WHERE
          s.setting_start_date IS NOT NULL
      )
  ),
  -- 2. Get the most recent alert log entry for each project and usage date to avoid duplicate alerts.
  cost_anomaly_alert_log_ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY
          project_id,
          usage_start_date
        ORDER BY
          alert_log_timestamp DESC
      ) AS rank
    FROM
      `<project_id>.<dataset>.cost_anomaly_alert_log`
  ),
  -- 3. Placeholder for project ownership and notification settings.
  gcp_project_ownership_temp AS (
    SELECT
      'project_id' AS project_id,
      'crutchfielda@google.com, sme@google.com' AS email_recipient_list,
      'C09EWA5FDMZ' AS slack_recipient_list
  )
-- Final selection and filtering of unalerted anomalies.
SELECT
  metrics.project_id AS project_id,
  metrics.project_name AS project_name,
  DATE(metrics.usage_start_date) AS anomaly_date,
  alert_log.alert_suppressed alert_log_alert_suppressed,
  alert_log.slack_alert_success alert_log_slack_alert_success,
  alert_log.alert_log_timestamp alert_log_timestamp,
  metrics.absolute_distance_from_threshold,
  metrics.absolute_percent_from_threshold,
  metrics.total_net_cost,
  metrics.anomaly_direction,
  metrics.upper_bound AS forecest_upper_bound,
  metrics.lower_bound AS forecast_lower_bound,
  coalesce(external_data.owner,'Owner Unknown') as owner,
  'C09EWA5FDMZ' AS slack_recipient_list
  --     ,external_data.base_data_cluster_fraas_reg as reg_environment_name,
  --     external_data.encore_tenant_ownership_tenantId as tenant_id,
  --     external_data.registration_environment_Subdomain as tenant_id_name,
  -- COALESCE(external_data.owner, 'default@example.com') AS email_recipient_list

FROM
  `<project_id>.<dataset>.vw_project_detect_anomalies_net_cost` AS metrics
  left join `<project_id>.<dataset>.vw_project_external_data_aggregated_v3` as external_data on metrics.project_id=external_data.project_id
  LEFT JOIN cost_anomaly_alert_log_ranked AS alert_log_ranked ON metrics.project_id = alert_log_ranked.project_id
  AND DATE(metrics.usage_start_date) = alert_log_ranked.usage_start_date
  AND alert_log_ranked.rank = 1
  LEFT JOIN `<project_id>.<dataset>.cost_anomaly_alert_log` AS alert_log ON alert_log_ranked.project_id = alert_log.project_id
  AND alert_log_ranked.usage_start_date = alert_log.usage_start_date
  AND alert_log_ranked.alert_log_timestamp = alert_log.alert_log_timestamp
  LEFT JOIN project_period_anomaly_threshold_settings_ranked AS settings ON metrics.project_id = settings.project_id
  AND DATE(metrics.usage_start_date) = DATE(settings.calendar_date)
  AND settings.rank = 1
  LEFT JOIN gcp_project_ownership_temp AS owners ON metrics.project_id = owners.project_id
WHERE
  -- Date range filter
  metrics.usage_start_date >= TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Los_Angeles'), INTERVAL 29 DAY)
  AND metrics.usage_start_date < TIMESTAMP_ADD(
    TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Los_Angeles'), INTERVAL 29 DAY),
    INTERVAL 30 DAY
  )
  -- Initial hardcoded filter: anomaly must be a cost increase exceeding default thresholds.
  AND metrics.is_anomaly
  AND metrics.anomaly_direction = 'Above'
  -- Alerting filter: ensure no alert was sent or suppressed.
  -- AND (
  --   alert_log.alert_log_timestamp IS NULL
  --   OR NOT (
  --     alert_log.alert_suppressed
  --     OR alert_log.slack_alert_success
  --   )
  -- )
  -- Dynamic threshold filter: anomaly must also exceed custom settings if they exist.
  AND metrics.absolute_distance_from_threshold > COALESCE(settings.absolute_delta_threshold, 150)
  AND metrics.absolute_percent_from_threshold >= COALESCE(settings.absolute_percent_threshold, 0.05)
  -- Max cost filter: suppress alerts if total cost does not exceed a custom max acceptable spend threshold. (For projects with expected anomalies, this is a way to turn off alerts on all but the highest spend)
  AND (
    settings.max_cost_threshold IS NULL
    OR metrics.total_net_cost > settings.max_cost_threshold
  )
-- GROUP BY
--   1,
--   2,
--   3,
--   4,
--   5,
--   6,
--   7,
--   8,
--   9,
--   10,
--   11
ORDER BY
  project_id ASC,
  anomaly_date ASC