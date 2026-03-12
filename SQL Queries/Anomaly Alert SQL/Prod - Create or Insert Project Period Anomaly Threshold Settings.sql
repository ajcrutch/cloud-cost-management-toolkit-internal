
--use this query to add new records and customize alert suppression thresholds on a project by project basis, applied across a range of dates
INSERT `<project_id>.<dataset>.project_period_anomaly_threshold_settings` (project_id,setting_start_date,setting_end_date,absolute_percent_threshold,absolute_delta_threshold,max_cost_threshold,setting_creation_timestamp)
VALUES(
  'project-id' --project_id
  , date(1901,01,01) --setting_start_date
  , date(2199,01,01) --setting_end_date /*can also be NULL to make this setting function indefinitely, or until a more recent setting creation timestamp is detected for that project*/
  , null --absolute_percent_threshold e.g. 0.15
  , null --absolute_delta_threshold e.g. 300
  , null --max_cost_threshold e.g. 10000, alerts suppressed unless spend is higher than this value
  , current_timestamp() --setting_creation_timestamp, do not change
  );

