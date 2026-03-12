-- Copied into a scheduled query on 2025-Nov-25 https://console.cloud.google.com/bigquery/scheduled-queries/locations/us/configs/693d8674-0000-2aa8-aa4f-14c14ee92ba0/details?project=<project_id>

-- DECLARE v_project_id STRING DEFAULT '<project_id>';
-- DECLARE v_dataset_id STRING DEFAULT 'gcp_billing';
-- DECLARE mdl_name STRING DEFAULT 'project_net_cost_forecast' ;

-- TODO: Make forecast horizon customizable
-- TODO: Make confidence customizable

-- STEP 1: create or replace the training data table - not necessary, already done in previous sql
-- CREATE OR REPLACE TABLE `<project_id>.<dataset>.TRAINING_DATA_project_net_cost_forecast` AS
-- SELECT
--   DATE(usage_start_date) as usage_start_date,
--   project_id,
--   total_net_cost
-- FROM vw_project_input_data_net_cost
-- WHERE DATE(usage_start_date) < DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

-- STEP 2: create or replace the ARIMA model
CREATE OR REPLACE MODEL `<project_id>.<dataset>.project_net_cost_forecast`
OPTIONS(  MODEL_TYPE = 'ARIMA_PLUS'
  ,  TIME_SERIES_TIMESTAMP_COL = 'usage_start_date'
  ,  TIME_SERIES_DATA_COL = 'total_net_cost'
  ,  TIME_SERIES_ID_COL = ['project_id']
  ,  HORIZON = 90 --horizon -- TODO: Make forecast horizon customizable
--    ,  HOLIDAY_REGION =  'US' -- Acceptable values are 'GLOBAL' or ['US', 'GB']. If enabled, update Step 4
  ,  AUTO_ARIMA = TRUE
  -- ,  FORECAST_LIMIT_LOWER_BOUND = 0 --Total net costs will never be negative - but cannot use this since this prevents model from running "ml.detect_anomalies" function
)
AS (SELECT usage_start_date,project_id,total_net_cost FROM `<project_id>.<dataset>.vw_project_input_data_net_cost`);

## STEP 3 AND 4: capture MODEL_INFO details including model_name & date_created (which will be used as model identifier in evaluation tables)
CREATE OR REPLACE TABLE `<project_id>.<dataset>.project_create_model_net_cost`
  (model_name           STRING
  , model_type          STRING
  , date_created        TIMESTAMP
  , horizon             INT64
  , holiday_region      STRING
  , bigquery_dataset    STRING);
  
INSERT `<project_id>.<dataset>.project_create_model_net_cost`
SELECT   'project_net_cost_forecast'
  , 'ARIMA_PLUS'
  , CURRENT_TIMESTAMP()
  , 90 --horizon -- TODO: Make forecast horizon customizable
  , 'TRUE' -- Update if HOLIDAY_REGION is enabled in Step 2.
  , '<project_id>.<dataset>';

-- STEP 5, 6, 7: update ANOMALY_DETECTION_BQML_MODEL_INFO from TRAINING_DATA, ML.TRAINING_INFO, and ML.FEATURE_INFO

CREATE TABLE IF NOT EXISTS `<project_id>.<dataset>.ANOMALY_DETECTION_BQML_MODEL_INFO`
  ( model_name STRING,
    model_type STRING,
    date_created TIMESTAMP,
    horizon INT64,
    holiday_region STRING,
    bigquery_dataset STRING,
    min_usage_start_date DATE,
    max_usage_start_date DATE,
    duration_ms INT64,
    feature_info ARRAY<STRUCT<input STRING, min FLOAT64, max FLOAT64, median FLOAT64, stddev FLOAT64, category_count INT64, null_count INT64>>,
    is_latest_model_version BOOL
  )
;

UPDATE `<project_id>.<dataset>.ANOMALY_DETECTION_BQML_MODEL_INFO`
SET is_latest_model_version = FALSE
WHERE model_name = 'project_net_cost_forecast'
AND is_latest_model_version = TRUE
;

INSERT `<project_id>.<dataset>.ANOMALY_DETECTION_BQML_MODEL_INFO`
WITH d AS (SELECT  MIN(usage_start_date) as min_date
                , MAX(usage_start_date) as max_date
          FROM `<project_id>.<dataset>.vw_project_input_data_net_cost`)
    ,t AS (SELECT duration_ms
          FROM ML.TRAINING_INFO(MODEL `<project_id>.<dataset>.project_net_cost_forecast`))
    ,f AS (SELECT ARRAY_AGG(STRUCT(input,min,max,median,stddev,category_count,null_count)) AS feature_info
          FROM ML.FEATURE_INFO(MODEL `<project_id>.<dataset>.project_net_cost_forecast`))

SELECT a.*
,d.min_date
,d.max_date
,t.duration_ms
,f.feature_info
,TRUE AS is_latest_model_version
FROM `<project_id>.<dataset>.project_create_model_net_cost` a
CROSS JOIN d
CROSS JOIN t
CROSS JOIN f
;



-- STEP 8, 9, 10: capture ML.ARIMA_EVALUATE and ML.ARIMA_COEFFICIENTS history for the model
CREATE TABLE IF NOT EXISTS `<project_id>.<dataset>.ARIMA_EVALUATE_project_net_cost_forecast`
  ( model_name STRING,
    date_created TIMESTAMP,
    project_id STRING,
    ar_coefficients ARRAY<FLOAT64>,
    ma_coefficients ARRAY<FLOAT64>,
    intercept_or_drift FLOAT64,
    non_seasonal_p INT64,
    non_seasonal_d INT64,
    non_seasonal_q INT64,
    has_drift BOOL,
    log_likelihood FLOAT64,
    AIC FLOAT64,
    variance FLOAT64,
    seasonal_periods ARRAY<STRING>,
    has_holiday_effect BOOL,
    has_spikes_and_dips BOOL,
    has_step_changes BOOL,
    error_message STRING,
    is_latest_model_version BOOL,
  )
;
UPDATE `<project_id>.<dataset>.ARIMA_EVALUATE_project_net_cost_forecast`
SET is_latest_model_version = FALSE
WHERE is_latest_model_version = TRUE
;

INSERT `<project_id>.<dataset>.ARIMA_EVALUATE_project_net_cost_forecast`
WITH c AS (SELECT *
          FROM ML.ARIMA_COEFFICIENTS(MODEL `<project_id>.<dataset>.project_net_cost_forecast`))
    ,e AS (SELECT *
          FROM ML.ARIMA_EVALUATE(MODEL `<project_id>.<dataset>.project_net_cost_forecast`))

SELECT m.model_name
,m.date_created
,c.project_id
,c.ar_coefficients
,c.ma_coefficients
,c.intercept_or_drift
,e.* EXCEPT(project_id)
, TRUE AS is_latest_model_version
FROM c
JOIN e on c.project_id = e.project_id
CROSS JOIN `<project_id>.<dataset>.project_create_model_net_cost` m
;

select * FROM `<project_id>.<dataset>.project_create_model_net_cost`;
        
create or replace view `<project_id>.<dataset>.vw_project_explain_forecast_net_cost` as
SELECT * FROM ML.EXPLAIN_FORECAST(MODEL `<project_id>.<dataset>.project_net_cost_forecast`,
    STRUCT(90 AS horizon, -- TODO: Make forecast horizon customizable
    0.99 AS confidence_level)); -- TODO: Make confidence customizable
