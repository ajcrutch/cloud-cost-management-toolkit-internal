SELECT 
-- sku.description
-- ,`labels`[SAFE_OFFSET(0)].`key`
--  resource.name
-- , resource.global_name
FORMAT_TIMESTAMP('%Y-%m-%d %H', usage_start_time, 'America/Los_Angeles') usage_date
,`labels`[SAFE_OFFSET(0)].`value` as data_source
,sum(cost) cost
,count(*) count
, sum(cost)/count(*) cost_per_query
-- ,sum(cost) total_cost
-- ,count(*) count_rows
 FROM `fr-billing.frsaas.gcp_billing_export_resource_v1_01A3C5_A1CCDD_9573E0` WHERE 
TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("2025-10-29") 
-- and FORMAT_TIMESTAMP('%Y-%m-%d %H', usage_start_time, 'America/Los_Angeles') in (
--   '2025-10-30 09','2025-10-29 14','2025-10-29 13','2025-10-29 09','2025-10-29 02')
and service.description='BigQuery'
and sku.description like '%Analysis%'
and project.id='<project_id>'
and `labels`[SAFE_OFFSET(0)].`key` = 'looker_studio_datasource_id'
and `labels`[SAFE_OFFSET(0)].`value` = 'e90032b9-502a-4169-918f-7d9cb90ed46at0c0d1'

group by 1,2
order by 4 desc
LIMIT 1000;

-- get job IDs
SELECT 
 detailed_billing.resource.name as job
 , `labels`[SAFE_OFFSET(0)].`value` as lookerstudio_datasource
 , FORMAT_TIMESTAMP('%Y-%m-%d', usage_start_time, 'America/Los_Angeles') usage_date
 , count(*)
 FROM `fr-billing.frsaas.gcp_billing_export_resource_v1_01A3C5_A1CCDD_9573E0` detailed_billing WHERE 
TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("2025-11-05") 
-- and FORMAT_TIMESTAMP('%Y-%m-%d %H', usage_start_time, 'America/Los_Angeles') in (
--   '2025-10-30 09','2025-10-29 14','2025-10-29 13','2025-10-29 09','2025-10-29 02')
and service.description='BigQuery'
and sku.description like '%Analysis%'
and project.id='<project_id>'
and `labels`[SAFE_OFFSET(0)].`key` = 'looker_studio_datasource_id'
-- and `labels`[SAFE_OFFSET(0)].`value` = 'e90032b9-502a-4169-918f-7d9cb90ed46at0c0d1'
-- and FORMAT_TIMESTAMP('%Y-%m-%d', usage_start_time, 'America/Los_Angeles') ='2025-11-04'
and cost>7
group by 1,2,3
order by FORMAT_TIMESTAMP('%Y-%m-%d', usage_start_time, 'America/Los_Angeles') desc
LIMIT 1000;



-- get job IDs
SELECT 
--  resource.name
`labels`[SAFE_OFFSET(0)].`value`
 , sum(cost)
 , count(*)
 FROM `fr-billing.frsaas.gcp_billing_export_resource_v1_01A3C5_A1CCDD_9573E0` WHERE 
TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) >= TIMESTAMP("2025-11-05") 
-- and FORMAT_TIMESTAMP('%Y-%m-%d %H', usage_start_time, 'America/Los_Angeles') in (
--   '2025-10-30 09','2025-10-29 14','2025-10-29 13','2025-10-29 09','2025-10-29 02')
and service.description='BigQuery'
and sku.description like '%Analysis%'
and project.id='<project_id>'
and `labels`[SAFE_OFFSET(0)].`key` = 'looker_studio_datasource_id'
and cost>7
-- and `labels`[SAFE_OFFSET(0)].`value` = 'e90032b9-502a-4169-918f-7d9cb90ed46at0c0d1'
-- and FORMAT_TIMESTAMP('%Y-%m-%d', usage_start_time, 'America/Los_Angeles') ='2025-11-04'
group by 1
order by 2 desc
LIMIT 1000;