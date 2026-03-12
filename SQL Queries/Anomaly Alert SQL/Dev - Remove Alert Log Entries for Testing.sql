-- a query which deletes all rows with alert timestamp from the past 24h from cost anomaly alert log table in the gcp_billing dataset
-- can this avoid the streaming buffer error somehow? perhaps there is a way to delete all the rows which might avoid this? ideally without needing to recreate the table

TRUNCATE TABLE `<project_id>.<dataset>.cost_anomaly_alert_log`;

-- DELETE
-- FROM
--   `<project_id>`.`<dataset>`.`cost_anomaly_alert_log`
-- WHERE
-- -- project_id= 'fr-6ossvmk4k2iujjsq9ols6okluo9'
--   TRUE;  -- This will delete all rows;

-- FROM
--   `<project_id>`.`<dataset>`.`cost_anomaly_alert_log`
-- WHERE
  -- alert_log_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  -- AND alert_log_timestamp < CURRENT_TIMESTAMP();
-- project_id= 'fr-6ossvmk4k2iujjsq9ols6okluo9'
-- 1=1