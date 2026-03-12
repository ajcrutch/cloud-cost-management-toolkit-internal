/*
This file creates the table function that assigns an anomaly ID
and identifies the most likely service.

--[UPDATED]--
This version is now also responsible for calculating the
"top 5 services by % increase" list, so the computation
is only done once.
*/

-- ============================================================================
-- Create Assign Anomaly ID Table Function
-- ============================================================================
CREATE OR REPLACE TABLE FUNCTION `<project_id>.<dataset>.assign_anomaly_id` (
    p_project_id STRING,
    p_target_date DATE
  ) AS (
  with
    -- Step 1: Gather cost data for the target project on specific historical dates.
    daily_costs AS (
      SELECT
        DATE(usage_start_date) AS usage_date,
        COALESCE(service_description, 'No Service Description') AS service_description,
        COALESCE(SUM(total_fcud_legacy), 0) + COALESCE(SUM(total_cost), 0) AS total_net_cost
      FROM
        `<project_id>.<dataset>.gcp_billing_export_daily_summary`
      WHERE
        COALESCE(project_id, 'No Project ID') = p_project_id
        AND usage_start_date IN (
          p_target_date,
          DATE_SUB(p_target_date, INTERVAL 1 DAY),
          DATE_SUB(p_target_date, INTERVAL 2 DAY),
          DATE_SUB(p_target_date, INTERVAL 3 DAY),
          DATE_SUB(p_target_date, INTERVAL 4 DAY),
          DATE_SUB(p_target_date, INTERVAL 7 DAY),
          DATE_SUB(p_target_date, INTERVAL 14 DAY),
          DATE_SUB(p_target_date, INTERVAL 21 DAY),
          DATE_SUB(p_target_date, INTERVAL 30 DAY)
        )
      GROUP BY
        1,
        2
    ),
    -- Step 2: Pivot the historical data
    cost_comparison AS (
      SELECT
        service_description,
        MAX(IF(usage_date = p_target_date, total_net_cost, 0)) AS cost_target_date,
        MAX(IF(usage_date = DATE_SUB(p_target_date, INTERVAL 1 DAY), total_net_cost, 0)) AS cost_1_day_ago,
        MAX(IF(usage_date = DATE_SUB(p_target_date, INTERVAL 2 DAY), total_net_cost, 0)) AS cost_2_days_ago,
        MAX(IF(usage_date = DATE_SUB(p_target_date, INTERVAL 3 DAY), total_net_cost, 0)) AS cost_3_days_ago,
        MAX(IF(usage_date = DATE_SUB(p_target_date, INTERVAL 4 DAY), total_net_cost, 0)) AS cost_4_days_ago,
        MAX(IF(usage_date = DATE_SUB(p_target_date, INTERVAL 7 DAY), total_net_cost, 0)) AS cost_7_days_ago,
        MAX(IF(usage_date = DATE_SUB(p_target_date, INTERVAL 14 DAY), total_net_cost, 0)) AS cost_14_days_ago,
        MAX(IF(usage_date = DATE_SUB(p_target_date, INTERVAL 21 DAY), total_net_cost, 0)) AS cost_21_days_ago,
        MAX(IF(usage_date = DATE_SUB(p_target_date, INTERVAL 30 DAY), total_net_cost, 0)) AS cost_30_days_ago
      FROM
        daily_costs
      GROUP BY
        service_description
    ),
    -- Step 3: Identify the service with the greatest *absolute* cost increase.
    anomaly_most_likely_service AS (
      SELECT
        service_description,
        GREATEST(
          cost_target_date - cost_1_day_ago,
          cost_target_date - cost_2_days_ago,
          cost_target_date - cost_3_days_ago,
          cost_target_date - cost_4_days_ago,
          cost_target_date - cost_7_days_ago,
          cost_target_date - cost_14_days_ago,
          cost_target_date - cost_21_days_ago,
          cost_target_date - cost_30_days_ago
        ) AS greatest_absolute_increase
      FROM
        cost_comparison
      ORDER BY
        greatest_absolute_increase DESC
      LIMIT
        1
    ),
    -- Step 4: Check if an alert has already been logged.
    latest_past_anomaly AS (
      SELECT
        log.anomaly_id
      FROM
        `<project_id>.<dataset>.cost_anomaly_alert_log` AS log
      JOIN
        anomaly_most_likely_service amls
        ON log.project_id = p_project_id
        AND log.most_likely_service = amls.service_description
      WHERE
        log.usage_start_date BETWEEN DATE_SUB(p_target_date, INTERVAL 7 DAY) AND p_target_date
      ORDER BY
        log.usage_start_date DESC
      LIMIT
        1
    ),
    -- ============================================================================
    -- Step 5: [NEW] Find the Top 5 services by *percentage* increase
    -- This re-uses the 'cost_comparison' CTE from Step 2
    -- ============================================================================
    top_services_ranked AS (
      SELECT
        service_description,
        -- Calculate percentage increase: (today - avg_past_3_days) / avg_past_3_days
        SAFE_DIVIDE(
          cost_target_date - ( (cost_1_day_ago + cost_2_days_ago + cost_3_days_ago) / 3 ),
          ( (cost_1_day_ago + cost_2_days_ago + cost_3_days_ago) / 3 )
        ) AS percent_increase
      FROM
        cost_comparison
      -- Ensure we have some cost to compare against to avoid divide-by-zero or skewed results
      WHERE
        (cost_1_day_ago + cost_2_days_ago + cost_3_days_ago) > 0
      ORDER BY
        percent_increase DESC
      LIMIT
        5
    ),
    -- Step 6: [NEW] Format this new service list into a string
    -- This re-uses the 'daily_costs' CTE from Step 1
    top_service_list_string AS (
      SELECT
        STRING_AGG(
          CONCAT(
            dc.service_description,
            ' (',
            FORMAT_DATE('%Y-%m-%d', dc.usage_date),
            ': $',
            CAST(dc.total_net_cost AS STRING),
            ')'
          ),
          ', '
          ORDER BY
            dc.service_description,
            dc.usage_date
        ) AS top_service_increase_list
      FROM
        daily_costs AS dc
      INNER JOIN
        top_services_ranked AS tsr
        ON dc.service_description = tsr.service_description
      WHERE
        dc.usage_date IN (
          p_target_date,
          DATE_SUB(p_target_date, INTERVAL 1 DAY),
          DATE_SUB(p_target_date, INTERVAL 2 DAY),
          DATE_SUB(p_target_date, INTERVAL 3 DAY)
        )
    )
    -- ============================================================================
  -- Final Step: Construct the output.
  SELECT
    COALESCE(
      lpa.anomaly_id,
      CONCAT(
        p_project_id,
        '~',
        CAST(p_target_date AS STRING),
        '~',
        amls.service_description
      )
    ) AS anomaly_id,
    (
      CASE
        WHEN lpa.anomaly_id IS NOT NULL THEN 'Yes'
        ELSE 'No'
      END
    ) AS anomaly_already_alerted,
    amls.service_description AS most_likely_service_description,
    -- [NEW] Add the formatted string to the output
    COALESCE(tsls.top_service_increase_list, 'No significant service increases found.') AS top_service_increase_list
  FROM
    anomaly_most_likely_service AS amls
    -- Use LEFT JOIN as the string might be empty
    LEFT JOIN
    top_service_list_string AS tsls
    ON 1 = 1
    LEFT JOIN
    latest_past_anomaly AS lpa
    ON 1 = 1
);

