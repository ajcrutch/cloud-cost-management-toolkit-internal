
-- SELECT
--   metrics.project_id AS project_id,
--   metrics.project_name AS project_name,
--   DATE(metrics.usage_start_date) AS anomaly_date,
--   metrics.absolute_distance_from_threshold,
--   metrics.absolute_percent_from_threshold,
--   metrics.total_net_cost AS total_cost,
--   metrics.anomaly_direction,
--   metrics.upper_bound AS forecest_upper_bound,
--   metrics.lower_bound AS forecast_lower_bound
-- FROM
--   `<project_id>.<dataset>.vw_project_detect_anomalies_net_cost` AS metrics
-- WHERE
--   -- Date range filter
--   metrics.usage_start_date >= TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Los_Angeles'), INTERVAL 29 DAY)
--   AND metrics.usage_start_date < TIMESTAMP_ADD(
--     TIMESTAMP_SUB(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/Los_Angeles'), INTERVAL 29 DAY),
--     INTERVAL 30 DAY
--   )
--   -- Initial hardcoded filter: anomaly must be a cost increase exceeding default thresholds.
--   AND metrics.is_anomaly
--   AND metrics.anomaly_direction = 'Above'
 
--   AND metrics.absolute_distance_from_threshold >  250
--   AND metrics.absolute_percent_from_threshold >=  0.25

-- GROUP BY
--   1,
--   2,
--   3,
--   4,
--   5,
--   6,
--   7,
--   8,
--   9
-- ORDER BY
--   metrics.absolute_distance_from_threshold DESC;

WITH AnomalyBuckets AS (
  SELECT
    CASE
      WHEN metrics.absolute_distance_from_threshold >= 0 AND metrics.absolute_distance_from_threshold < 50 THEN '$0-49'
      WHEN metrics.absolute_distance_from_threshold >= 50 AND metrics.absolute_distance_from_threshold < 100 THEN '$50-99'
      WHEN metrics.absolute_distance_from_threshold >= 100 AND metrics.absolute_distance_from_threshold < 150 THEN '$100-149'
      WHEN metrics.absolute_distance_from_threshold >= 150 AND metrics.absolute_distance_from_threshold < 200 THEN '$150-199'
      WHEN metrics.absolute_distance_from_threshold >= 200 AND metrics.absolute_distance_from_threshold < 250 THEN '$200-249'
      WHEN metrics.absolute_distance_from_threshold >= 250 AND metrics.absolute_distance_from_threshold < 300 THEN '$250-299'
      WHEN metrics.absolute_distance_from_threshold >= 300 AND metrics.absolute_distance_from_threshold < 350 THEN '$300-349'
      WHEN metrics.absolute_distance_from_threshold >= 350 AND metrics.absolute_distance_from_threshold < 400 THEN '$350-399'
      WHEN metrics.absolute_distance_from_threshold >= 400 AND metrics.absolute_distance_from_threshold < 450 THEN '$400-449'
      WHEN metrics.absolute_distance_from_threshold >= 450 AND metrics.absolute_distance_from_threshold <= 500 THEN '$450-500'
      ELSE 'Over $500'
    END AS distance_bucket,
    CASE
      WHEN metrics.absolute_distance_from_threshold >= 0 AND metrics.absolute_distance_from_threshold < 50 THEN '01'
      WHEN metrics.absolute_distance_from_threshold >= 50 AND metrics.absolute_distance_from_threshold < 100 THEN '02'
      WHEN metrics.absolute_distance_from_threshold >= 100 AND metrics.absolute_distance_from_threshold < 150 THEN '03'
      WHEN metrics.absolute_distance_from_threshold >= 150 AND metrics.absolute_distance_from_threshold < 200 THEN '04'
      WHEN metrics.absolute_distance_from_threshold >= 200 AND metrics.absolute_distance_from_threshold < 250 THEN '05'
      WHEN metrics.absolute_distance_from_threshold >= 250 AND metrics.absolute_distance_from_threshold < 300 THEN '06'
      WHEN metrics.absolute_distance_from_threshold >= 300 AND metrics.absolute_distance_from_threshold < 350 THEN '07'
      WHEN metrics.absolute_distance_from_threshold >= 350 AND metrics.absolute_distance_from_threshold < 400 THEN '08'
      WHEN metrics.absolute_distance_from_threshold >= 400 AND metrics.absolute_distance_from_threshold < 450 THEN '09'
      WHEN metrics.absolute_distance_from_threshold >= 450 AND metrics.absolute_distance_from_threshold <= 500 THEN '10'
      ELSE '11' 
    END AS distant_bucket_order,
    COUNT(*) AS count_anomalies,
    AVG(metrics.absolute_distance_from_threshold) AS avg_distance,
    AVG(metrics.absolute_percent_from_threshold) AS avg_percent
  FROM
    `<project_id>.<dataset>.vw_project_detect_anomalies_net_cost` AS metrics
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
  GROUP BY 1, 2
)
SELECT
  distance_bucket,
  SUM(count_anomalies) OVER (ORDER BY distant_bucket_order DESC) AS cumulative_anomalies,
  count_anomalies,
  FORMAT('$%.2f', avg_distance) AS avg_distance,
  FORMAT('%.2f%%', avg_percent * 100) AS avg_percent,
  distant_bucket_order
FROM
  AnomalyBuckets
ORDER BY
  distant_bucket_order DESC;

