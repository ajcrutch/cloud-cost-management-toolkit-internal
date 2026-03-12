/*
This file contains the one-time setup for your remote function (Cloud Run)
and remote model (Gemini).
*/

-- ============================================================================
-- 1. Create Remote Function (for Cloud Run)
-- ============================================================================
CREATE OR REPLACE FUNCTION `<project_id>.<dataset>.anomaly_alert` (
    anomaly_details_json STRING
  )
  RETURNS JSON
  REMOTE WITH CONNECTION `<project_id>.us.cloud_services_connection`
  OPTIONS (
    endpoint = 'https://gcp-billing-anomaly-alert-930308978288.us-central1.run.app'
  );

-- ============================================================================
-- 2. Create Remote Model (for Gemini)
-- ============================================================================
CREATE OR REPLACE MODEL `<project_id>.<dataset>.gemini_recommender`
  REMOTE WITH CONNECTION `<project_id>.us.cloud_services_connection`
  OPTIONS (
    endpoint = 'gemini-2.5-pro'
  );